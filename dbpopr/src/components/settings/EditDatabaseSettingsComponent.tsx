import React, {useEffect, useState} from "react"
import PageHeader from "../pageheader/PageHeader";
import {DatabaseConfiguration, getSettings, postDatabase} from "../../api/settingsApi";
import {Button} from "react-bootstrap";
import {useParams} from "react-router";

export default function EditDatabaseSettingsComponent() {
    let {type} = useParams();
    const [disabled, setDisabled] = useState(false);
    const [url, setUrl] = useState("");
    const [username, setUsername] = useState("");
    const [password, setPassword] = useState("*****");
    const [conflict] = useState(false);
    const [loading, setLoading] = useState(true);
    const [saving, setSaving] = useState(false);
    const [error, setError] = useState<string | undefined>();
    const title = type === "source" ? "Source Database" : "Target Database";

    useEffect(() => {
        getSettings()
            .then(result => {
                let settings = result.data;
                let configuration: DatabaseConfiguration;
                if (type === "source") {
                    configuration = settings.sourceDatabaseConfiguration;
                } else {
                    configuration = settings.targetDatabaseConfiguration;
                }
                setDisabled(configuration.disabled);
                setUrl(configuration.url);
                setUsername(configuration.username);
                setLoading(false)
            })
    }, [type])


    if (loading) return <div><i className={"fa fa-spinner fa-spin"}/> Loading...</div>

    function whenSave(event: React.SyntheticEvent) {
        event.preventDefault();
        setError(undefined);
        setSaving(true);
        try {
            postDatabase(type!, {disabled, url, username, password, conflict}).then(() => {
                console.log("saved")
            }).catch(reason => {
                setError(reason.response.data.detail);
            }).finally(() => {
                setSaving(false);
            })
        } catch (e) {
            console.log("Error?");
        }
    }

    return <>
        <PageHeader title={title}
                    breadcrumbs={
                        [
                            {to: "/settings", label: "Settings"},
                            {label: title},
                        ]
                    }
        />

        <form onSubmit={whenSave}>
            <div className="form-check form-switch mb-3">
                <input className="form-check-input" type="checkbox" role="switch" id="enabled" checked={!disabled} onChange={event => setDisabled(!event.target.checked)}/>
                <label className="form-check-label" htmlFor="enabled">Enabled</label>
            </div>
            {disabled || <>
                {conflict && <div className="alert alert-warning" role="alert">There is a conflict between environment variables and the configuration file.</div>}
                <div className="form-group">
                    <label htmlFor="url">URL</label>
                    <input type="text" className="form-control" id="url" placeholder="jdbc:sqlserver://localhost;database=tempdb;trustServerCertificate=true"
                           value={url}
                           onChange={event => setUrl(event.target.value)}
                    />
                </div>
                <div className="form-group">
                    <label htmlFor="username">Username</label>
                    <input type="username" className="form-control" id="username" placeholder="sa"
                           value={username}
                           onChange={event => setUsername(event.target.value)}
                    />
                </div>
                <div className="form-group">
                    <label htmlFor="password">Password</label>
                    <input type="password" className="form-control" id="password"
                           value={password}
                           onChange={event => setPassword(event.target.value)}
                    />
                </div>
            </>
            }
            <div className={"mt-5"}>
                {error && <div className="alert alert-danger" role="alert">{error}</div>}
                {saving && <div className="alert alert-info" role="alert"><i className={"fa fa-spinner fa-spin"}/> Validating...</div>}
                <Button type={"submit"} disabled={saving}>
                    Save
                </Button>
            </div>
        </form>
    </>
}