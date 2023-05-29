import axios, {AxiosResponse} from "axios";
import {Button} from "react-bootstrap";
import React from "react";

export default function SqlForm({setLoading, setState}: { setLoading: (b: boolean) => void, setState: (state: string) => void }) {

    function handleDownload() {
        setLoading(true);
        axios.post("/deploy/script/sql", {}, {responseType: 'blob'})
            .then(response => {
                downloadFile(response);
                setLoading(false);
                setState("sql-downloaded")
            });
    }

    function downloadFile(response: AxiosResponse<any>) {
        // Thanks to https://stackoverflow.com/a/53230807/310923
        const href = URL.createObjectURL(response.data);
        const link = document.createElement('a');
        link.href = href;
        // Thanks to https://stackoverflow.com/a/50642818/310923
        const headerLine = response.headers['content-disposition'];
        if (headerLine) {
            const startFileNameIndex = headerLine.indexOf('"') + 1
            const endFileNameIndex = headerLine.lastIndexOf('"');
            const filename = headerLine.substring(startFileNameIndex, endFileNameIndex);
            link.setAttribute('download', filename);
        } else {
            link.setAttribute('download', 'file.zip');
        }
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(href);
    }

    return <>
        <Button type="submit" className="btn btn-primary" onClick={handleDownload}>
            Download
        </Button>
    </>
}
