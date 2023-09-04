import React, {useState} from "react"
import LoadingOverlay from "../../utils/LoadingOverlay";
import PageHeader from "../../pageheader/PageHeader";
import {Button} from "react-bootstrap";
import axios from "axios";

export default function DeployResetComponent() {
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState();
    const [isReset, setIsReset] = useState(false);

    function whenReset() {
        setLoading(true);
        setError(undefined);
        axios.post('/deploy/reset')
            .then(() => setIsReset(true))
            .catch(reason => setError(reason.response.data?.detail || 'Error'))
            .finally(() => setLoading(false));
    }

    if (loading) return <div><i className={"fa fa-spinner fa-spin"}/> Loading...</div>;

    function BeforeReset() {
        return <div className={"container"}>
            <p>
                Click the button below if you want to capture the current state of the code directory as the baseline for deployments.<br/>
                It will delete the snapshot.zip file, as well as the FlywayDB scripts.
            </p>
            <div className={"mt-5 ms-5"}>
                <Button variant={"danger"} onClick={whenReset}>
                    <i className={"fa fa-undo"}/> Reset
                </Button>
            </div>
            {error && <div className="mt-5 alert alert-danger" role="alert">{error}</div>}
        </div>
    }

    function AfterReset() {
        return <div className={"container"}>
            <p>
                The deployment has been reset.
            </p>
        </div>
    }

    return <div id={"deployment-reset-component"}>
        <LoadingOverlay active={loading}/>
        <div className={"container"}>
            <PageHeader title={"Deployment Reset"} subtitle={"Reset deployment to factory settings."}/>
            {isReset ? <AfterReset/> : <BeforeReset/>}
        </div>
    </div>
}