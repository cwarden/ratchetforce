// Ratchet processor to send JSON to Salesforce using force cli.
// The active force user and API version will be used.  Use `force active` and
// `force apiversion` to check and set.
package ratchetforce

import (
	"fmt"
	force "github.com/heroku/force/lib"

	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/util"
)

type ForceWriter struct {
	force    *force.Force
	endpoint string
}

func NewForceTransform(endpoint string) (*ForceWriter, error) {
	return NewForceWriter(endpoint)
}

// Any REST API endpoint that allows JSON to be POSTed can be used.
// Use the path after the API version, e.g. /composite to send to
// /services/data/v40.0/composite
func NewForceWriter(endpoint string) (*ForceWriter, error) {
	session, err := force.ActiveForce()
	if err != nil {
		return nil, fmt.Errorf("Unable to initialize force connection: %s", err)
	}

	loader := &ForceWriter{
		force:    session,
		endpoint: endpoint,
	}

	return loader, nil
}

func (w *ForceWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	result, err := w.force.PostREST(w.endpoint, string(d))
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("Unable to send to Salesforce: %s", err), killChan)
	}
	outputChan <- []byte(result)
}

func (w *ForceWriter) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (w *ForceWriter) String() string {
	return "ForceWriter"
}
