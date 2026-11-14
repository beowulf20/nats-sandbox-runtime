// Package pythonprofile defines the opt-in trusted import sets shared by the
// runtime and its SDK. Empty selects the original generic interpreter.
package pythonprofile

import "fmt"

const (
	NumPy   = "numpy"
	PlotAgg = "plot-agg"
)

func Validate(profile string) error {
	switch profile {
	case "", NumPy, PlotAgg:
		return nil
	default:
		return fmt.Errorf("unsupported import profile %q", profile)
	}
}
