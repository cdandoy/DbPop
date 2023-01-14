import React from "react";
import {Pages} from "./DownloadComponent";
import BackNextComponent from "./BackNextComponent";

export const SelectModes = {"model": "model", "bulk": "bulk"};
export default function SelectModeComponent({mode, setMode, setPage}: {
    mode: string;
    setMode: ((s: string) => void);
    setPage: ((s: string) => void);
}) {
    function onNext() {
        if (mode === SelectModes.bulk) setPage(Pages.selectBulkTables);
    }

    return <>
        <div>
            <BackNextComponent onNext={onNext}/>

            <fieldset>
                <div className="form-check">
                    <input className="form-check-input"
                           type="radio"
                           name="radioModel"
                           id="radioModel"
                           autoFocus={true}
                           checked={mode === SelectModes.model}
                           onChange={() => setMode(SelectModes.model)}/>
                    <label className="form-check-label" htmlFor="radioModel">
                        Download a model
                    </label>
                </div>
                    <div className="form-check">
                        <input className="form-check-input"
                               type="radio"
                               name="radioBulk"
                               id="radioBulk"
                               checked={mode === SelectModes.bulk}
                               onChange={() => setMode(SelectModes.bulk)}/>
                        <label className="form-check-label" htmlFor="radioBulk">
                            Bulk Download
                        </label>
                    </div>
            </fieldset>
        </div>
    </>
}