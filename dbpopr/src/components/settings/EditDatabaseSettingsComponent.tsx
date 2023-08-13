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
    const [password, setPassword] = useState("");
    const [conflict] = useState(false);
    const [loading, setLoading] = useState(true);
    const [saving, setSaving] = useState(false);
    const [error, setError] = useState<string | undefined>();
    const title = type === "source" ? "Source Database" : "Target Database";

    useEffect(() => {
        setLoading(true);
        loadSettings();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [type]);

    function loadSettings() {
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
                setPassword(configuration.password || "");
                setLoading(false)
                setSaving(false);
            })
    }

    function whenSave(event: React.SyntheticEvent) {
        event.preventDefault();
        setError(undefined);
        setSaving(true);
        try {
            postDatabase(type!, {disabled, url, username, password, conflict}).then(() => {
                loadSettings();
            }).catch(reason => {
                setError(reason.response.data.detail);
                setSaving(false);
            })
        } catch (e) {
            console.log("Error?");
        }
    }

    function getTitle(): JSX.Element | string | undefined {
        if (type === "source") {
            return <>
                <div>
                    DbPop can copy tables, data, and code from a source database to files.<br/>
                    The source database is typically a copy of the production database, use your production database at your own risks.
                </div>


            </>
        } else if (type === "target") {
            return "DbPop seeds data into the target database."
        }
    }

    if (loading) return <div><i className={"fa fa-spinner fa-spin"}/> Loading...</div>

    return <>
        <PageHeader title={title}
                    subtitle={getTitle()}
                    breadcrumbs={
                        [
                            {to: "/settings", label: "Settings"},
                            {label: title},
                        ]
                    }
        />

        <form onSubmit={whenSave} autoComplete={"off"}>
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
                    <input type="text" className="form-control" id="username" placeholder="sa"
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