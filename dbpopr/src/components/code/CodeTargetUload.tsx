import React, {useEffect, useState} from "react";
import PageHeader from "../pageheader/PageHeader";
import LoadingOverlay from "../utils/LoadingOverlay";
import {uploadFileToTarget} from "../../api/codeApi";
import {Alert} from "react-bootstrap";

export default function CodeTargetUload() {
    const [loading, setLoading] = useState(false);
    const [loaded, setLoaded] = useState(false);
    const [error, setError] = useState<string | undefined>();

    useEffect(() => {
        if (!loaded) {
            setLoading(true);
            setError(undefined);
            uploadFileToTarget()
                .then(() => setLoaded(true))
                .catch((error) => setError(error.response.statusText))
                .finally(() => setLoading(false));
        }
    }, [])

    return <div id={"code-source-download"}>
        <PageHeader title={"Upload to Target"} subtitle={"Create"}/>
        <LoadingOverlay active={loading}/>
        {error && <Alert variant={"danger"}>{error}</Alert>}
        {!error && loaded && <Alert variant={"success"} className={"text-center"}>Created</Alert>}
    </div>;
}