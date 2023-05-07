import React, {useEffect, useState} from "react";
import PageHeader from "../pageheader/PageHeader";
import LoadingOverlay from "../utils/LoadingOverlay";
import {DownloadResult, downloadTargetToFile} from "../../api/codeApi";
import {Alert} from "react-bootstrap";
import download_target from "./download_target.png"

export default function CodeTargetDownload() {
    const [loading, setLoading] = useState(false);
    const [downloadResult, setDownloadResult] = useState<DownloadResult | undefined>();
    const [error, setError] = useState<string | undefined>();

    useEffect(() => {
        setLoading(true);
        setError(undefined);
        setDownloadResult(undefined);
        downloadTargetToFile()
            .then((result) => setDownloadResult(result.data))
            .catch((error) => setError(error.response.statusText))
            .finally(() => setLoading(false));
    }, [])

    return <div id={"code-target-download"}>
        <PageHeader title={"Download from Target"} subtitle={"Download the tables and sprocs from the Target database to the SQL files"} tool={<img src={download_target} style={{width: "20em"}} alt={"Download from Target"}/>}/>
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
                                <td className={"text-end"}>{pair.right?.toLocaleString()}</td>
                            </tr>
                        ))
                    }
                    </tbody>
                </table>
            </>
        )}
    </div>;
}