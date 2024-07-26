package file

import (
	"fmt"
	"kis-flow/common"
	"kis-flow/kis"
	"os"

	yaml "gopkg.in/yaml.v3"
)

// ConfigExportYaml exports the flow configuration and saves it locally
func ConfigExportYaml(flow kis.Flow, savePath string) error {
	data, err := yaml.Marshal(flow.GetConfig())
	if err != nil {
		return err
	}

	// flow
	err = os.WriteFile(savePath+common.KisIdTypeFlow+"-"+flow.GetName()+".yaml", data, 0644)
	if err != nil {
		return err
	}

	// function
	for _, fp := range flow.GetConfig().Flows {
		fConf := flow.GetFuncConfigByName(fp.FuncName)
		if fConf == nil {
			return fmt.Errorf("function name = %s config is nil ", fp.FuncName)
		}

		fData, err := yaml.Marshal(fConf)
		if err != nil {
			return err
		}

		if err = os.WriteFile(savePath+common.KisIdTypeFunction+"-"+fp.FuncName+".yaml", fData, 0644); err != nil {
			return err
		}

		// Connector
		if fConf.Option.CName != "" {
			cConf, err := fConf.GetConnConfig()
			if err != nil {
				return err
			}

			cdata, err := yaml.Marshal(cConf)
			if err != nil {
				return err
			}

			if err = os.WriteFile(savePath+common.KisIdTypeConnector+"-"+cConf.CName+".yaml", cdata, 0644); err != nil {
				return err
			}
		}
	}

	return nil
}
