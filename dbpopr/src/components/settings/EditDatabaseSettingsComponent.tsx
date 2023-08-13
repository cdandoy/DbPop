import React, {useContext, useEffect, useState} from "react"
import PageHeader from "../pageheader/PageHeader";
import {DatabaseConfigurationResponse, getSettings, postDatabase} from "../../api/settingsApi";
import {Button} from "react-bootstrap";
import {useParams} from "react-router";
import {useNavigate} from "react-router-dom";
import {WebSocketStateContext} from "../ws/useWebSocketState";

export default function EditDatabaseSettingsComponent() {
    let {type} = useParams();
    const siteStatus = useContext(WebSocketStateContext);
    const siteStatusErrorMessage = type === "source" ? siteStatus.sourceErrorMessage : siteStatus.targetErrorMessage;
    const [url, setUrl] = useState("");
    const [username, setUsername] = useState("");
    const [password, setPassword] = useState("");
    const [fromEnvVariables, setFromEnvVariables] = useState(false);
    const [loading, setLoading] = useState(true);
    const [saving, setSaving] = useState(false);
    const [error, setError] = useState<string | undefined>(siteStatusErrorMessage);
    const title = type === "source" ? "Source Database" : "Target Database";
    let navigate = useNavigate();

    useEffect(() => {
        setLoading(true);
        getSettings()
            .then(result => {
                let settings = result.data;
                let configuration: DatabaseConfigurationResponse;
                if (type === "source") {
                    configuration = settings.sourceDatabaseConfiguration;
                } else {
                    configuration = settings.targetDatabaseConfiguration;
                }
                setUrl(configuration.url || '');
                setUsername(configuration.username || '');
                setPassword(configuration.password || '');
                setFromEnvVariables(configuration.fromEnvVariables);
                setLoading(false)
                setSaving(false);
            })
    }, [type]);

    function whenSave(event: React.SyntheticEvent) {
        event.preventDefault();
        doSave();
    }

    function doSave() {
        setError(undefined);
        setSaving(true);
        try {
            postDatabase(type!, {url, username, password})
                .then(() => {
                    navigate('/settings');
                })
                .catch(reason => {
                    setError(reason.response.data.detail);
                    setSaving(false);
                })
        } catch (e) {
            console.log("Error?");
        }
    }

    function whenClear() {
        postDatabase(type!, {})
            .then(() => {
                navigate('/settings')
            });
    }

    function getTitle(): JSX.Element | string | undefined {
        if (type === "source") {
            return <div>
                DbPop can copy tables, data, and code from a source database to files.<br/>
                The source database is typically a copy of the production database, use your production database at your own risks.
            </div>
        } else if (type === "target") {
            return <div>
                DbPop seeds data into the target database.
            </div>
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
            <div className="form-group">
                <label htmlFor="url">URL</label>
                {fromEnvVariables ?
                    <span className="form-control">{url}</span>
                    :
                    <input type="text" className="form-control" id="url" placeholder="jdbc:sqlserver://localhost;database=tempdb;trustServerCertificate=true"
                           value={url}
                           onChange={event => setUrl(event.target.value)}
                    />
                }
            </div>
            <div className="form-group">
                <label htmlFor="username">Username</label>
                {fromEnvVariables ?
                    <span className="form-control">{username}</span>
                    :
                    <input type="text" className="form-control" id="username" placeholder="sa"
                           value={username}
                           onChange={event => setUsername(event.target.value)}
                    />
                }
            </div>
            <div className="form-group">
                <label htmlFor="password">Password</label>
                {fromEnvVariables ?
                    <span className="form-control">{password}</span>
                    :
                    <input type="password" className="form-control" id="password"
                           value={password}
                           onChange={event => setPassword(event.target.value)}
                    />
                }
            </div>
            <div className={"mt-5"}>
                {error && <div className="alert alert-danger" role="alert">{error}</div>}
                {fromEnvVariables ?
                    <div className="alert alert-info " role="alert">Defined by environment variables.</div>
                    :
                    <>
                        {saving && <div className="alert alert-info" role="alert"><i className={"fa fa-spinner fa-spin"}/> Validating...</div>}
                        <div className="btn-group" role="group" aria-label="Button group example">
                            <Button type={"submit"} disabled={saving}>
                                Save
                            </Button>
                            <Button type={"button"} disabled={saving} variant={"secondary"} onClick={whenClear}>
                                Clear
                            </Button>
                        </div>
                    </>
                }
            </div>
        </form>
    </>
}