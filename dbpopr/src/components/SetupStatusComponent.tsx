import React from "react";
import {SetupState} from "./app/SetupState";

export function SetupStatusComponent({setupState}: { setupState: SetupState }) {

    return (
        <div className={"m-3"}>
            {setupState.error == null && (
                <div><i className="fa fa-fw fa-spinner fa-spin"/>&nbsp;{setupState.activity}</div>
            )}
            {setupState.error != null && (<>
                <div>{setupState.activity}:</div>
                <pre className="m-3 alert alert-danger" role="alert" style={{whiteSpace: "pre-line"}}>{setupState.error}</pre>
            </>)}
        </div>
    )
}