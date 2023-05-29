import {Button} from "react-bootstrap";
import React from "react";

export function NoSnapshotContent({handleCreateSnapshot}: { handleCreateSnapshot: () => void }) {
    return <div>
        <p>You must create a baseline snapshot before you can use this feature.</p>
        <Button variant={"primary"} onClick={handleCreateSnapshot}>
            Create Snapshot
        </Button>
    </div>
}