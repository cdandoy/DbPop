import React, {useEffect, useState} from "react";
import PageHeader from "../../../pageheader/PageHeader";
import LoadingOverlay from "../../../utils/LoadingOverlay";
import {DownloadResult, downloadSourceToFile} from "../../../../api/codeApi";
import {Alert} from "react-bootstrap";
import download_source from "./download_source.png"

export default function CodeSourceDownload() {
    const [loading, setLoading] = useState(false);
    const [downloadResult, setDownloadResult] = useState<DownloadResult | undefined>();
    const [error, setError] = useState<string | undefined>();

    useEffect(() => {
        setLoading(true);
        setError(undefined);
        setDownloadResult(undefined);
        downloadSourceToFile()
            .then((result) => setDownloadResult(result.data))
            .catch((error) => setError(error.response.statusText))
            .finally(() => setLoading(false));
    }, [])

    return <div id={"code-source-download"}>
        <PageHeader title={"Download Source"} subtitle={"Download the tables and sprocs from the source database to the local SQL files"} tool={<img src={download_source} style={{width: "20em"}} alt={"Download Source"}/>}/>
        <LoadingOverlay active={loading}/>
        {error && <Alert variant={"danger"}>{error}</Alert>}
        {!error && downloadResult && (
            <>
                <Alert variant={"success"} className={"text-center"}>Downloaded</Alert>
                <table className={"table-hover ms-5"}>
                    <tbody>
                    {
                        downloadResult.codeTypeCounts.map(pair => (
                            <tr key={pair.left}>
                                <td>{pair.left}:</td>
                                <td>{pair.right}</td>
                            </tr>
                        ))
                    }
                    </tbody>
                </table>
            </>
        )}
    </div>;
}