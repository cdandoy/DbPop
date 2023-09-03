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
            .catch((reason) => setError(reason.response.data?.detail || 'Error'))
            .finally(() => setLoading(false));
    }, [])

    return <div id={"code-source-download"} className={"container"}>
        <PageHeader title={"Download Source"} subtitle={"Download the tables and code definitions from the source database to the local SQL files"} tool={<img src={download_source} style={{width: "20em"}} alt={"Download Source"}/>}/>
        <LoadingOverlay active={loading}/>
        {error && <Alert variant={"danger"}>{error}</Alert>}
        {!error && downloadResult && (
            <>
                <Alert variant={"success"} className={"text-center"}>Downloaded to {downloadResult.downloadedPath}</Alert>
                <table className={"table-hover ms-5"}>
                    <tbody>
                    {
                        downloadResult.codeTypeCounts.map(pair => (
                            <tr key={pair.left}>
                                <td>{pair.left}:</td>
                                <td>
                                    <div className={"text-end ms-3"}>
                                        {((pair.right) as number).toLocaleString()}
                                    </div>
                                </td>
                            </tr>
                        ))
                    }
                    </tbody>
                </table>
            </>
        )}
    </div>;
}