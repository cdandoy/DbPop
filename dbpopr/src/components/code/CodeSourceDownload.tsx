import React, {useEffect, useState} from "react";
import PageHeader from "../pageheader/PageHeader";
import LoadingOverlay from "../utils/LoadingOverlay";
import {downloadSourceToFile} from "../../api/codeApi";
import {Alert} from "react-bootstrap";

export default function CodeSourceDownload() {
    const [loading, setLoading] = useState(false);
    const [loaded, setLoaded] = useState(false);
    const [error, setError] = useState<string | undefined>();

    useEffect(() => {
        if (!loaded) {
            setLoading(true);
            setError(undefined);
            downloadSourceToFile()
                .then(() => setLoaded(true))
                .catch((error) => setError(error))
                .finally(() => setLoading(false));
        }
    }, [])

    return <div id={"code-source-download"}>
        <PageHeader title={"Source Download"} subtitle={"Dowload the tables and stored procedures from the source database to the local filesystem."}/>
        <LoadingOverlay active={loading}/>
        {error && <Alert variant={"danger"}>{error}</Alert>}
        {!error && loaded && <Alert variant={"success"} className={"text-center"}>Downloaded</Alert>}
    </div>;
}