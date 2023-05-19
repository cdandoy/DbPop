import React from "react";
import "./ServerDisconnected.scss"

export function ServerDisconnected() {
    return <>
        <div id={"server-disconnected-component"}>
            <h1>
                <i className={"fa fa-spinner fa-spin"}/>
                Connecting...
            </h1>
        </div>
    </>
}
