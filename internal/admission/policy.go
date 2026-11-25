// Package admission compiles and evaluates the CEL in the runner-pool
// ValidatingAdmissionPolicy offline, against Pod fixtures.
//
// The policy at hosts/ms-a2/github-runner-pool/admission.yaml is the boundary
// that keeps the GitHub App private key out of a job VM. Until this package
// existed nothing evaluated it: kubeconform checks schemas and not policies,
// and kubectl --dry-run=server needs a live API server with the policy already
// installed. The structural checks in verify-manifests.py catch a rule that was
// deleted; they cannot catch a rule that is present and wrong, which is the
// failure that already happened once -- the first version of the worker
// environment rule denied every legitimate worker.
//
// The expressions are read from the committed YAML at run time and never
// copied into Go source. A copy is the drift this package exists to prevent.
//
// # How faithful this is to the API server
//
// This is a second evaluator, not the API server's evaluator, and the
// difference is worth naming rather than assuming away:
//
//   - It is cel-go at the version k8s.io/apiserver pins for the cluster's
//     Kubernetes minor (see go.mod), so the language level and the standard
//     library match the admission plugin's.
//   - The six names the admission plugin binds for a policy expression --
//     object, oldObject, request, params, authorizer, namespaceObject -- are
//     all declared, plus variables. Only object and variables are bound, so an
//     expression reaching for one of the others fails loudly here instead of
//     evaluating against a fabricated value.
//   - object is dyn-typed, decoded from the fixture YAML. The API server types
//     it from the OpenAPI schema, so a field-name typo it would reject at
//     policy-creation time compiles here and evaluates to "no such key". A
//     missing-field mistake therefore shows up as a denial in these tests
//     rather than as a compile error, which is louder, not quieter.
//   - Integers decode as int64 rather than float64, so a comparison against an
//     integer literal means what it means against a typed Kubernetes object.
//   - Variables are evaluated eagerly in declaration order. The API server
//     evaluates them lazily, and in both cases a variable may only reference
//     variables declared before it.
//   - failurePolicy: Fail is modelled by returning an error for an expression
//     that faults or returns a non-bool, rather than treating the fault as a
//     pass.
package admission

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
	"sigs.k8s.io/yaml"
)

// KubernetesExpressionVariables are the names the Kubernetes admission plugin
// declares for a ValidatingAdmissionPolicy expression, plus the variables map
// the policy's own spec.variables entries are reached through.
var KubernetesExpressionVariables = []string{
	"object",
	"oldObject",
	"request",
	"params",
	"authorizer",
	"namespaceObject",
	"variables",
}

// NamedExpression is a spec.variables or spec.matchConditions entry.
type NamedExpression struct {
	Name       string `json:"name"`
	Expression string `json:"expression"`
}

// Validation is one spec.validations entry.
type Validation struct {
	Expression        string `json:"expression"`
	Message           string `json:"message"`
	MessageExpression string `json:"messageExpression"`
}

// Label is what a denial by this validation is reported as. The message is the
// policy's own stable identifier; the expression is the fallback for a rule
// that carries neither a message nor a messageExpression.
func (v Validation) Label() string {
	if v.Message != "" {
		return v.Message
	}
	if v.MessageExpression != "" {
		return v.MessageExpression
	}
	return v.Expression
}

// Policy is the parsed ValidatingAdmissionPolicy.
type Policy struct {
	Name            string
	FailurePolicy   string
	MatchConditions []NamedExpression
	Variables       []NamedExpression
	Validations     []Validation
}

// Load reads the ValidatingAdmissionPolicy out of a manifest file that may hold
// several documents.
func Load(path string) (*Policy, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	for _, doc := range splitDocuments(raw) {
		if len(bytes.TrimSpace(doc)) == 0 {
			continue
		}
		var parsed struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Spec struct {
				FailurePolicy   string            `json:"failurePolicy"`
				MatchConditions []NamedExpression `json:"matchConditions"`
				Variables       []NamedExpression `json:"variables"`
				Validations     []Validation      `json:"validations"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal(doc, &parsed); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if parsed.Kind != "ValidatingAdmissionPolicy" {
			continue
		}
		return &Policy{
			Name:            parsed.Metadata.Name,
			FailurePolicy:   parsed.Spec.FailurePolicy,
			MatchConditions: parsed.Spec.MatchConditions,
			Variables:       parsed.Spec.Variables,
			Validations:     parsed.Spec.Validations,
		}, nil
	}
	return nil, fmt.Errorf("%s holds no ValidatingAdmissionPolicy", path)
}

func splitDocuments(raw []byte) [][]byte {
	var (
		docs    [][]byte
		current [][]byte
	)
	for _, line := range bytes.Split(raw, []byte("\n")) {
		if string(bytes.TrimRight(line, "\r")) == "---" {
			docs = append(docs, bytes.Join(current, []byte("\n")))
			current = nil
			continue
		}
		current = append(current, line)
	}
	return append(docs, bytes.Join(current, []byte("\n")))
}

// DecodeObject turns a Pod manifest into the shape a CEL expression walks.
// Integral numbers become int64 rather than float64, so a comparison against an
// integer literal behaves as it does against a typed Kubernetes object.
func DecodeObject(raw []byte) (map[string]any, error) {
	encoded, err := yaml.YAMLToJSON(raw)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	object, ok := normalizeNumbers(decoded).(map[string]any)
	if !ok {
		return nil, fmt.Errorf("manifest is not an object")
	}
	return object, nil
}

func normalizeNumbers(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			typed[key] = normalizeNumbers(item)
		}
		return typed
	case []any:
		for index, item := range typed {
			typed[index] = normalizeNumbers(item)
		}
		return typed
	case json.Number:
		if integer, err := typed.Int64(); err == nil {
			return integer
		}
		float, err := typed.Float64()
		if err != nil {
			return typed.String()
		}
		return float
	default:
		return value
	}
}

type compiledExpression struct {
	name    string
	source  string
	program cel.Program
}

type compiledValidation struct {
	label   string
	rule    compiledExpression
	message *compiledExpression
}

// Evaluator holds the compiled policy.
type Evaluator struct {
	policy          *Policy
	matchConditions []compiledExpression
	variables       []compiledExpression
	validations     []compiledValidation
}

// Result is one admission decision. Denied holds the label of every validation
// that did not pass, so a fixture denied by two rules cannot read as a fixture
// denied by the one rule it was written for.
type Result struct {
	Matched bool
	Denied  []string
}

// Admitted reports whether the policy let the object through.
func (r Result) Admitted() bool { return len(r.Denied) == 0 }

// NewEvaluator compiles every expression the policy declares. A policy whose
// CEL does not compile is a policy that fails open or closed for the wrong
// reason, so compilation failure is returned rather than deferred to
// evaluation.
func NewEvaluator(policy *Policy) (*Evaluator, error) {
	options := []cel.EnvOption{cel.OptionalTypes(), ext.Strings()}
	for _, name := range KubernetesExpressionVariables {
		options = append(options, cel.Variable(name, cel.DynType))
	}
	env, err := cel.NewEnv(options...)
	if err != nil {
		return nil, err
	}
	evaluator := &Evaluator{policy: policy}
	for _, condition := range policy.MatchConditions {
		compiled, err := compileBool(env, "matchCondition "+condition.Name, condition.Expression)
		if err != nil {
			return nil, err
		}
		evaluator.matchConditions = append(evaluator.matchConditions, compiled)
	}
	for _, variable := range policy.Variables {
		compiled, err := compileAny(env, "variable "+variable.Name, variable.Expression)
		if err != nil {
			return nil, err
		}
		compiled.name = variable.Name
		evaluator.variables = append(evaluator.variables, compiled)
	}
	for index, validation := range policy.Validations {
		rule, err := compileBool(env, fmt.Sprintf("validation %d (%s)", index, validation.Label()), validation.Expression)
		if err != nil {
			return nil, err
		}
		entry := compiledValidation{label: validation.Label(), rule: rule}
		if validation.MessageExpression != "" {
			message, err := compileAny(env, "messageExpression for "+validation.Label(), validation.MessageExpression)
			if err != nil {
				return nil, err
			}
			entry.message = &message
		}
		evaluator.validations = append(evaluator.validations, entry)
	}
	return evaluator, nil
}

// Policy returns the policy this evaluator compiled.
func (e *Evaluator) Policy() *Policy { return e.policy }

func compileAny(env *cel.Env, what, source string) (compiledExpression, error) {
	return compile(env, what, source, false)
}

func compileBool(env *cel.Env, what, source string) (compiledExpression, error) {
	return compile(env, what, source, true)
}

func compile(env *cel.Env, what, source string, requireBool bool) (compiledExpression, error) {
	ast, issues := env.Compile(source)
	if issues != nil && issues.Err() != nil {
		return compiledExpression{}, fmt.Errorf("%s does not compile: %w", what, issues.Err())
	}
	// dyn is accepted because object is dyn-typed here; the runtime check in
	// evaluateBool is what refuses a non-bool value.
	if output := ast.OutputType().String(); requireBool && output != "bool" && output != "dyn" {
		return compiledExpression{}, fmt.Errorf("%s returns %s, not bool", what, output)
	}
	program, err := env.Program(ast)
	if err != nil {
		return compiledExpression{}, fmt.Errorf("%s does not build: %w", what, err)
	}
	return compiledExpression{name: what, source: source, program: program}, nil
}

// Evaluate judges one object. An expression that faults or returns a non-bool
// is an error rather than a pass, which is what failurePolicy: Fail means.
func (e *Evaluator) Evaluate(object map[string]any) (Result, error) {
	activation := map[string]any{"object": object}
	for _, condition := range e.matchConditions {
		matched, err := evaluateBool(condition, activation)
		if err != nil {
			return Result{}, err
		}
		if !matched {
			return Result{Matched: false}, nil
		}
	}
	variables := map[string]any{}
	activation["variables"] = variables
	for _, variable := range e.variables {
		value, _, err := variable.program.Eval(activation)
		if err != nil {
			return Result{}, fmt.Errorf("%s faulted: %w", variable.name, err)
		}
		variables[variable.name] = value
	}
	result := Result{Matched: true}
	for _, validation := range e.validations {
		passed, err := evaluateBool(validation.rule, activation)
		if err != nil {
			return Result{}, err
		}
		if passed {
			continue
		}
		result.Denied = append(result.Denied, e.denialLabel(validation, activation))
	}
	return result, nil
}

func (e *Evaluator) denialLabel(validation compiledValidation, activation map[string]any) string {
	if validation.message == nil {
		return validation.label
	}
	value, _, err := validation.message.program.Eval(activation)
	if err != nil {
		return validation.label
	}
	text, ok := value.Value().(string)
	if !ok || strings.TrimSpace(text) == "" {
		return validation.label
	}
	return text
}

func evaluateBool(expression compiledExpression, activation map[string]any) (bool, error) {
	value, _, err := expression.program.Eval(activation)
	if err != nil {
		return false, fmt.Errorf("%s faulted: %w", expression.name, err)
	}
	result, ok := value.Value().(bool)
	if !ok {
		return false, fmt.Errorf("%s returned %T, not bool", expression.name, value.Value())
	}
	return result, nil
}
