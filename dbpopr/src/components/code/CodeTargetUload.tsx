import React, {useEffect, useState} from "react";
import PageHeader from "../pageheader/PageHeader";
import LoadingOverlay from "../utils/LoadingOverlay";
import {uploadFileToTarget, UploadResult} from "../../api/codeApi";
import {Alert} from "react-bootstrap";
import "./CodeTargetUload.scss"
import upload_target from "./upload_target.png"

export default function CodeTargetUload() {
    const [loading, setLoading] = useState(false);
    const [uploadResult, setUploadResult] = useState<UploadResult | undefined>();
    const [errorCount, setErrorCount] = useState(0);
    const [error, setError] = useState<string | undefined>();

    useEffect(() => {
        setLoading(true);
        setUploadResult(undefined);
        setError(undefined);
        uploadFileToTarget()
            .then((result) => {
                const uploadResult: UploadResult = result.data;
                setUploadResult(result.data);
                setErrorCount(uploadResult.fileExecutions.filter(it => it.error).length)
            })
            .catch((error) => setError(error.response.statusText))
            .finally(() => setLoading(false));
    }, [])

    return <div id={"code-source-download"}>
        <PageHeader title={"Upload to Target"} subtitle={"Upload the tables and sprocs from the SQL files to the Target database"} tool={<img src={upload_target} style={{width: "20em"}} alt={"image"}/>}/>
        <LoadingOverlay active={loading}/>
        {error && <Alert variant={"danger"}>{error}</Alert>}

        {uploadResult && errorCount == 0 && (
            <Alert variant={"success"} className={"text-center"}>
                <div>Executed in {uploadResult.fileExecutions.length} files {uploadResult.executionTime}ms</div>
            </Alert>
        )}

        {uploadResult && errorCount > 0 && (
            <>
                <Alert variant={"danger"} className={"text-center"}>
                    <div>Executed in {uploadResult.fileExecutions.length} files {uploadResult.executionTime}ms</div>
                    <div>{errorCount} errors encountered</div>
                </Alert>
                <div className={"ms-5"}>
                    {uploadResult.fileExecutions
                        .filter(fileExecution => fileExecution.error)
                        .map(fileExecution => (
                            <div key={fileExecution.filename}>
                                <div>{fileExecution.filename}</div>
                                <div className={"ms-3 mb-3"}>{fileExecution.error}</div>
                            </div>
                        ))}
                </div>
            </>
        )}
    </div>;
}