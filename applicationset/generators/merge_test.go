package generators

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	argoprojiov1alpha1 "github.com/argoproj/argo-cd/v2/pkg/apis/application/v1alpha1"
)

const (
	defaultTestMergeKey      = "b"
	defaultTestMergeKeyValue = "bbb"
)

type testMergeParams map[string]any

type newParamOption func(m map[string]any)

func flattenT[T any](in ...[]T) []T {
	return lo.Flatten(in)
}

func withParamOption(params ...string) newParamOption {
	if len(params)/2 != 0 {
		panic(fmt.Errorf("params needs to be pairs of key & value"))
	}

	return func(m map[string]any) {
		for i := 0; i < len(params); i += 2 {
			m[params[i]] = params[i+1]
		}
	}
}

func newParam(options ...newParamOption) map[string]any {
	m := map[string]any{
		"a":                 "aaa",
		defaultTestMergeKey: defaultTestMergeKeyValue,
	}

	for _, opt := range options {
		opt(m)
	}

	return m
}

func newTestGenerator[T argoprojiov1alpha1.ApplicationSetNestedGenerator | argoprojiov1alpha1.ApplicationSetTerminalGenerator](params ...testMergeParams) *T {
	elements := make([]apiextensionsv1.JSON, len(params))

	for i, param := range params {
		jbytes, err := json.Marshal(param)
		if err != nil {
			panic(fmt.Errorf("marshalling param: %w", err))
		}
		elements[i] = apiextensionsv1.JSON{Raw: jbytes}
	}

	var val T
	switch any(val).(type) {
	case argoprojiov1alpha1.ApplicationSetNestedGenerator:
		val = any(argoprojiov1alpha1.ApplicationSetNestedGenerator{
			List: &argoprojiov1alpha1.ListGenerator{
				Elements: elements,
			},
		}).(T)
	case argoprojiov1alpha1.ApplicationSetTerminalGenerator:
		val = any(argoprojiov1alpha1.ApplicationSetTerminalGenerator{
			List: &argoprojiov1alpha1.ListGenerator{
				Elements: elements,
			},
		}).(T)
	}

	return (any(&val)).(*T)
}

func getNestedListGenerator2(params ...testMergeParams) *argoprojiov1alpha1.ApplicationSetNestedGenerator {
	elements := make([]apiextensionsv1.JSON, len(params))

	for i, param := range params {
		jbytes, err := json.Marshal(param)
		if err != nil {
			panic(fmt.Errorf("marshalling param: %w", err))
		}
		elements[i] = apiextensionsv1.JSON{Raw: jbytes}
	}

	generator := argoprojiov1alpha1.ApplicationSetNestedGenerator{
		List: &argoprojiov1alpha1.ListGenerator{
			Elements: elements,
		},
	}

	return &generator
}

func getTerminalListGenerator2(params ...testMergeParams) *argoprojiov1alpha1.ApplicationSetTerminalGenerator {
	elements := make([]apiextensionsv1.JSON, len(params))

	for i, param := range params {
		jbytes, err := json.Marshal(param)
		if err != nil {
			panic(fmt.Errorf("marshalling param: %w", err))
		}
		elements[i] = apiextensionsv1.JSON{Raw: jbytes}
	}

	generator := argoprojiov1alpha1.ApplicationSetTerminalGenerator{
		List: &argoprojiov1alpha1.ListGenerator{
			Elements: elements,
		},
	}

	return &generator
}

func getNestedListGenerator(json string) *argoprojiov1alpha1.ApplicationSetNestedGenerator {
	return &argoprojiov1alpha1.ApplicationSetNestedGenerator{
		List: &argoprojiov1alpha1.ListGenerator{
			Elements: []apiextensionsv1.JSON{{Raw: []byte(json)}},
		},
	}
}

func getTerminalListGeneratorMultiple(jsons []string) argoprojiov1alpha1.ApplicationSetTerminalGenerator {
	elements := make([]apiextensionsv1.JSON, len(jsons))

	for i, json := range jsons {
		elements[i] = apiextensionsv1.JSON{Raw: []byte(json)}
	}

	generator := argoprojiov1alpha1.ApplicationSetTerminalGenerator{
		List: &argoprojiov1alpha1.ListGenerator{
			Elements: elements,
		},
	}

	return generator
}

func listOfMapsToSet(maps []map[string]interface{}) (map[string]bool, error) {
	set := make(map[string]bool, len(maps))
	for _, paramMap := range maps {
		paramMapAsJson, err := json.Marshal(paramMap)
		if err != nil {
			return nil, err
		}

		set[string(paramMapAsJson)] = false
	}
	return set, nil
}

func TestMergeGenerate(t *testing.T) {
	type testCase struct {
		name           string
		baseGenerators []argoprojiov1alpha1.ApplicationSetNestedGenerator
		mergeKeys      []string
		mergeMode      string
		expectedErr    error
		expected       []map[string]interface{}
	}

	testCases := []testCase{
		{
			name:           "no generators",
			baseGenerators: []argoprojiov1alpha1.ApplicationSetNestedGenerator{},
			mergeKeys:      []string{"b"},
			expectedErr:    ErrLessThanTwoGeneratorsInMerge,
		},
		{
			name: "one generator",
			baseGenerators: []argoprojiov1alpha1.ApplicationSetNestedGenerator{
				*getNestedListGenerator(`{"a": "1_1","b": "same","c": "1_3"}`),
			},
			mergeKeys:   []string{"b"},
			expectedErr: ErrLessThanTwoGeneratorsInMerge,
		},
		{
			name: "happy flow - generate paramSets",
			baseGenerators: []argoprojiov1alpha1.ApplicationSetNestedGenerator{
				*getNestedListGenerator(`{"a": "1_1","b": "same","c": "1_3"}`),
				*getNestedListGenerator(`{"a": "2_1","b": "same"}`),
				*getNestedListGenerator(`{"a": "3_1","b": "different","c": "3_3"}`), // gets ignored because its merge key value isn't in the base params set
			},
			mergeKeys: []string{"b"},
			expected: []map[string]interface{}{
				{"a": "2_1", "b": "same", "c": "1_3"},
			},
		},
		{
			name: "merge keys absent - do not merge",
			baseGenerators: []argoprojiov1alpha1.ApplicationSetNestedGenerator{
				*getNestedListGenerator(`{"a": "a"}`),
				*getNestedListGenerator(`{"a": "a"}`),
			},
			mergeKeys: []string{"b"},
			expected: []map[string]interface{}{
				{"a": "a"},
			},
		},
		{
			name: "merge key present in first set, absent in second - do not merge",
			baseGenerators: []argoprojiov1alpha1.ApplicationSetNestedGenerator{
				*getNestedListGenerator(`{"a": "a"}`),
				*getNestedListGenerator(`{"b": "b"}`),
			},
			mergeKeys: []string{"b"},
			expected: []map[string]interface{}{
				{"a": "a"},
			},
		},
		{
			name: "merge nested matrix with some lists",
			baseGenerators: []argoprojiov1alpha1.ApplicationSetNestedGenerator{
				{
					Matrix: toAPIExtensionsJSON(t, &argoprojiov1alpha1.NestedMatrixGenerator{
						Generators: []argoprojiov1alpha1.ApplicationSetTerminalGenerator{
							getTerminalListGeneratorMultiple([]string{`{"a": "1"}`, `{"a": "2"}`}),
							getTerminalListGeneratorMultiple([]string{`{"b": "1"}`, `{"b": "2"}`}),
						},
					}),
				},
				*getNestedListGenerator(`{"a": "1", "b": "1", "c": "added"}`),
			},
			mergeKeys: []string{"a", "b"},
			expected: []map[string]interface{}{
				{"a": "1", "b": "1", "c": "added"},
				{"a": "1", "b": "2"},
				{"a": "2", "b": "1"},
				{"a": "2", "b": "2"},
			},
		},
		{
			name: "merge nested merge with some lists",
			baseGenerators: []argoprojiov1alpha1.ApplicationSetNestedGenerator{
				{
					Merge: toAPIExtensionsJSON(t, &argoprojiov1alpha1.NestedMergeGenerator{
						MergeKeys: []string{"a"},
						Generators: []argoprojiov1alpha1.ApplicationSetTerminalGenerator{
							getTerminalListGeneratorMultiple([]string{`{"a": "1", "b": "1"}`, `{"a": "2", "b": "2"}`}),
							getTerminalListGeneratorMultiple([]string{`{"a": "1", "b": "3", "c": "added"}`, `{"a": "3", "b": "2"}`}), // First gets merged, second gets ignored
						},
					}),
				},
				*getNestedListGenerator(`{"a": "1", "b": "3", "d": "added"}`),
			},
			mergeKeys: []string{"a", "b"},
			expected: []map[string]interface{}{
				{"a": "1", "b": "3", "c": "added", "d": "added"},
				{"a": "2", "b": "2"},
			},
		},
	}

	// mergeMode
	for _, mergeMode := range []string{"left-join", "inner-join", "full-join"} {
		tc := func() testCase {
			a2 := []string{"a", "a222"}
			c := []string{"c", "ccc"}
			d := []string{"d", "ddd"}

			expected := newParam(withParamOption(flattenT(a2, c, d)...))
			return testCase{
				name: fmt.Sprintf("mergeMode - %s - given left param with key, given right param with key, expect merged", mergeMode),
				baseGenerators: []argoprojiov1alpha1.ApplicationSetNestedGenerator{
					*newTestGenerator[argoprojiov1alpha1.ApplicationSetNestedGenerator](newParam(
						withParamOption(d...))),
					*newTestGenerator[argoprojiov1alpha1.ApplicationSetNestedGenerator](newParam(
						withParamOption(flattenT(a2, c)...))),
				},
				mergeKeys: []string{defaultTestMergeKey},
				mergeMode: mergeMode,
				expected: []map[string]interface{}{
					expected,
				},
			}
		}()

		testCases = append(testCases, tc)
	}

	testCases = append(testCases, []testCase{
		func() testCase {
			mergeMode := "left-join"

			a2 := []string{"a", "a222"}
			c := []string{"c", "ccc"}

			expected := newParam(withParamOption("d", "ddd"))
			return testCase{
				name: fmt.Sprintf("mergeMode - %s - given left param with key, given right param without key, expect right dropped/ignored", mergeMode),
				baseGenerators: []argoprojiov1alpha1.ApplicationSetNestedGenerator{
					*newTestGenerator[argoprojiov1alpha1.ApplicationSetNestedGenerator](expected),
					*newTestGenerator[argoprojiov1alpha1.ApplicationSetNestedGenerator](newParam(
						withParamOption(flattenT(a2, c, []string{defaultTestMergeKey, "something else"})...))),
				},
				mergeKeys: []string{defaultTestMergeKey},
				expected: []map[string]interface{}{
					expected,
				},
			}
		}(),
		func() testCase {
			mergeMode := "inner-join"

			first := newParam()
			second := newParam(withParamOption(defaultTestMergeKey, "something else"))
			return testCase{
				name: fmt.Sprintf("mergeMode - %s - given left param with key, given right param without key, expect neither", mergeMode),
				baseGenerators: []argoprojiov1alpha1.ApplicationSetNestedGenerator{
					*newTestGenerator[argoprojiov1alpha1.ApplicationSetNestedGenerator](first),
					*newTestGenerator[argoprojiov1alpha1.ApplicationSetNestedGenerator](second),
				},
				mergeKeys: []string{defaultTestMergeKey},
				mergeMode: mergeMode,
				expected:  []map[string]interface{}{},
			}
		}(),
		func() testCase {
			mergeMode := "full-join"

			expected1 := newParam()
			expected2 := newParam(withParamOption(defaultTestMergeKey, "something else", "d", "ddd"))
			return testCase{
				name: fmt.Sprintf("mergeMode - %s - given left param with key, given right param without key, expect separate param results", mergeMode),
				baseGenerators: []argoprojiov1alpha1.ApplicationSetNestedGenerator{
					*newTestGenerator[argoprojiov1alpha1.ApplicationSetNestedGenerator](expected1),
					*newTestGenerator[argoprojiov1alpha1.ApplicationSetNestedGenerator](expected2),
				},
				mergeKeys: []string{defaultTestMergeKey},
				mergeMode: mergeMode,
				expected: []map[string]interface{}{
					expected1,
					expected2,
				},
			}
		}(),
	}...)

	for _, testCase := range testCases {
		testCaseCopy := testCase // since tests may run in parallel

		t.Run(testCaseCopy.name, func(t *testing.T) {
			t.Parallel()

			appSet := &argoprojiov1alpha1.ApplicationSet{}

			mergeGenerator := NewMergeGenerator(
				map[string]Generator{
					"List": &ListGenerator{},
					"Matrix": &MatrixGenerator{
						supportedGenerators: map[string]Generator{
							"List": &ListGenerator{},
						},
					},
					"Merge": &MergeGenerator{
						supportedGenerators: map[string]Generator{
							"List": &ListGenerator{},
						},
					},
				},
			)

			got, err := mergeGenerator.GenerateParams(&argoprojiov1alpha1.ApplicationSetGenerator{
				Merge: &argoprojiov1alpha1.MergeGenerator{
					Generators: testCaseCopy.baseGenerators,
					MergeKeys:  testCaseCopy.mergeKeys,
					Template:   argoprojiov1alpha1.ApplicationSetTemplate{},
				},
			}, appSet, nil)

			if testCaseCopy.expectedErr != nil {
				require.EqualError(t, err, testCaseCopy.expectedErr.Error())
			} else {
				expectedSet, err := listOfMapsToSet(testCaseCopy.expected)
				require.NoError(t, err)

				actualSet, err := listOfMapsToSet(got)
				require.NoError(t, err)

				require.NoError(t, err)
				assert.Equal(t, expectedSet, actualSet)
			}
		})
	}
}

func toAPIExtensionsJSON(t *testing.T, g interface{}) *apiextensionsv1.JSON {
	resVal, err := json.Marshal(g)
	if err != nil {
		t.Error("unable to unmarshal json", g)
		return nil
	}

	res := &apiextensionsv1.JSON{Raw: resVal}

	return res
}

func TestParamSetsAreUniqueByMergeKeys(t *testing.T) {
	testCases := []struct {
		name        string
		mergeKeys   []string
		paramSets   []map[string]interface{}
		expectedErr error
		expected    map[string]map[string]interface{}
	}{
		{
			name:        "no merge keys",
			mergeKeys:   []string{},
			expectedErr: ErrNoMergeKeys,
		},
		{
			name:      "no paramSets",
			mergeKeys: []string{"key"},
			expected:  make(map[string]map[string]interface{}),
		},
		{
			name:      "simple key, unique paramSets",
			mergeKeys: []string{"key"},
			paramSets: []map[string]interface{}{{"key": "a"}, {"key": "b"}},
			expected: map[string]map[string]interface{}{
				`{"key":"a"}`: {"key": "a"},
				`{"key":"b"}`: {"key": "b"},
			},
		},
		{
			name:      "simple key object, unique paramSets",
			mergeKeys: []string{"key"},
			paramSets: []map[string]interface{}{{"key": map[string]interface{}{"hello": "world"}}, {"key": "b"}},
			expected: map[string]map[string]interface{}{
				`{"key":{"hello":"world"}}`: {"key": map[string]interface{}{"hello": "world"}},
				`{"key":"b"}`:               {"key": "b"},
			},
		},
		{
			name:        "simple key, non-unique paramSets",
			mergeKeys:   []string{"key"},
			paramSets:   []map[string]interface{}{{"key": "a"}, {"key": "b"}, {"key": "b"}},
			expectedErr: fmt.Errorf("%w. Duplicate key was %s", ErrNonUniqueParamSets, `{"key":"b"}`),
		},
		{
			name:      "simple key, duplicated key name, unique paramSets",
			mergeKeys: []string{"key", "key"},
			paramSets: []map[string]interface{}{{"key": "a"}, {"key": "b"}},
			expected: map[string]map[string]interface{}{
				`{"key":"a"}`: {"key": "a"},
				`{"key":"b"}`: {"key": "b"},
			},
		},
		{
			name:        "simple key, duplicated key name, non-unique paramSets",
			mergeKeys:   []string{"key", "key"},
			paramSets:   []map[string]interface{}{{"key": "a"}, {"key": "b"}, {"key": "b"}},
			expectedErr: fmt.Errorf("%w. Duplicate key was %s", ErrNonUniqueParamSets, `{"key":"b"}`),
		},
		{
			name:      "compound key, unique paramSets",
			mergeKeys: []string{"key1", "key2"},
			paramSets: []map[string]interface{}{
				{"key1": "a", "key2": "a"},
				{"key1": "a", "key2": "b"},
				{"key1": "b", "key2": "a"},
			},
			expected: map[string]map[string]interface{}{
				`{"key1":"a","key2":"a"}`: {"key1": "a", "key2": "a"},
				`{"key1":"a","key2":"b"}`: {"key1": "a", "key2": "b"},
				`{"key1":"b","key2":"a"}`: {"key1": "b", "key2": "a"},
			},
		},
		{
			name:      "compound key object, unique paramSets",
			mergeKeys: []string{"key1", "key2"},
			paramSets: []map[string]interface{}{
				{"key1": "a", "key2": map[string]interface{}{"hello": "world"}},
				{"key1": "a", "key2": "b"},
				{"key1": "b", "key2": "a"},
			},
			expected: map[string]map[string]interface{}{
				`{"key1":"a","key2":{"hello":"world"}}`: {"key1": "a", "key2": map[string]interface{}{"hello": "world"}},
				`{"key1":"a","key2":"b"}`:               {"key1": "a", "key2": "b"},
				`{"key1":"b","key2":"a"}`:               {"key1": "b", "key2": "a"},
			},
		},
		{
			name:      "compound key, duplicate key names, unique paramSets",
			mergeKeys: []string{"key1", "key1", "key2"},
			paramSets: []map[string]interface{}{
				{"key1": "a", "key2": "a"},
				{"key1": "a", "key2": "b"},
				{"key1": "b", "key2": "a"},
			},
			expected: map[string]map[string]interface{}{
				`{"key1":"a","key2":"a"}`: {"key1": "a", "key2": "a"},
				`{"key1":"a","key2":"b"}`: {"key1": "a", "key2": "b"},
				`{"key1":"b","key2":"a"}`: {"key1": "b", "key2": "a"},
			},
		},
		{
			name:      "compound key, non-unique paramSets",
			mergeKeys: []string{"key1", "key2"},
			paramSets: []map[string]interface{}{
				{"key1": "a", "key2": "a"},
				{"key1": "a", "key2": "a"},
				{"key1": "b", "key2": "a"},
			},
			expectedErr: fmt.Errorf("%w. Duplicate key was %s", ErrNonUniqueParamSets, `{"key1":"a","key2":"a"}`),
		},
		{
			name:      "compound key, duplicate key names, non-unique paramSets",
			mergeKeys: []string{"key1", "key1", "key2"},
			paramSets: []map[string]interface{}{
				{"key1": "a", "key2": "a"},
				{"key1": "a", "key2": "a"},
				{"key1": "b", "key2": "a"},
			},
			expectedErr: fmt.Errorf("%w. Duplicate key was %s", ErrNonUniqueParamSets, `{"key1":"a","key2":"a"}`),
		},
	}

	for _, testCase := range testCases {
		testCaseCopy := testCase // since tests may run in parallel

		t.Run(testCaseCopy.name, func(t *testing.T) {
			t.Parallel()

			got, err := getParamSetsByMergeKey(testCaseCopy.mergeKeys, testCaseCopy.paramSets)

			if testCaseCopy.expectedErr != nil {
				require.EqualError(t, err, testCaseCopy.expectedErr.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, testCaseCopy.expected, got)
			}
		})
	}
}
