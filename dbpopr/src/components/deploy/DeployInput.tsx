import React, {useState} from "react";
import SqlForm from "./SqlForm";
import FlywayForm from "./FlywayForm";
import {Change} from "../../api/changeApi";
import {NavLink} from "react-router-dom";

export function DeployInput({setLoading, codeChanges, setState, setFlywayGeneratedFilename, defaultType}: {
    setLoading: (b: boolean) => void
    codeChanges: Change[]
    setState: (s: string) => void
    setFlywayGeneratedFilename: (s: string) => void
    defaultType: string
}) {
    const [type, setType] = useState(defaultType);
    return <div className={"row"}>
        <div className={"col-6"}>
            {codeChanges?.length > 0 && <div className="alert alert-warning" role="alert">
                <i className={"fa fa-exclamation-triangle"}/> You have <NavLink to={"/codechanges"}>changes</NavLink> in the database that have not been saved to the code directory.
            </div>}
            <div className={"mb-3"}>
                <label htmlFor="deployType">Deployment Type</label>
                <select className="form-select" defaultValue={type} onChange={e => setType(e.target.value)}>
                    <option value={"SQL"}>SQL Scripts</option>
                    <option value={"FLYWAY"}>Flyway</option>
                </select>
                {type === "SQL"
                    ? <small className="form-text text-muted">Generates a pair of SQL Scripts to deploy and undeploy your changes.</small>
                    : <small className="form-text text-muted">Generates a <a href={"https://flywaydb.org/"} target={"_blank"}>Flyway</a> script.</small>
                }
            </div>

            {type === "SQL" && <SqlForm setLoading={setLoading} setState={setState}/>}
            {type === "FLYWAY" && <FlywayForm setLoading={setLoading} setState={setState} setFlywayGeneratedFilename={setFlywayGeneratedFilename}/>}
        </div>
    </div>;
}