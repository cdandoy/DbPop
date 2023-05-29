import React, {useState} from "react";
import axios, {AxiosResponse} from "axios";
import {Button} from "react-bootstrap";

interface ScriptFlywayRequest {
    name: string;
}

interface ScriptFlywayResponse {
    generatedFile: string;
}

export default function FlywayForm({setLoading, setState, setFlywayGeneratedFilename}: {
    setLoading: (b: boolean) => void
    setState: (state: string) => void
    setFlywayGeneratedFilename: (s: string) => void
}) {
    const [name, setName] = useState("");

    function handleGenerate() {
        setLoading(true)
        axios.post<ScriptFlywayRequest, AxiosResponse<ScriptFlywayResponse>>("/deploy/script/flyway", {name})
            .then((response) => {
                setFlywayGeneratedFilename(response.data.generatedFile);
                setState("flyway-created");
            })
            .finally(() => setLoading(false));
    }

    return <>
        <div className={"mb-3"}>
            <label htmlFor="flyway-descr">Description</label>
            <input type="text" className="form-control" id="flyway-descr" placeholder="Version 1" value={name} onChange={e => setName(e.target.value)}/>
            <small id="flyway-descr=help" className="form-text text-muted">Description to include in the Flyway script filename.</small>
        </div>
        <Button type="submit" className="btn btn-primary" onClick={handleGenerate}>
            Generate
        </Button>
    </>
}
