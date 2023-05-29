import React from "react"
import {Button} from "react-bootstrap";

export default function SqlDownloaded({snapshotFilename, onCancel, onOk}: { snapshotFilename: string, onCancel: () => void, onOk: () => void }) {
    return <div>
        <h5>SQL Files Downloaded</h5>
        <p>Please examine the downloaded ZIP file, it contains the SQL script to deploy your changes as well as a script to undo those changes.</p>
        <p>Click the Snapshot button to create a new snapshot: {snapshotFilename}</p>
        <div>
            <Button variant={"light"} className={"ms-3"} onClick={() => onCancel()}>
                Cancel
            </Button>
            <Button variant={"danger"} className={"ms-3"} onClick={onOk}>
                Snapshot
            </Button>
        </div>
    </div>
}