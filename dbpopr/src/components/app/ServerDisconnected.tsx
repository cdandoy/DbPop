import React, {useEffect, useState} from "react";
import "./ServerDisconnected.scss"

export function ServerDisconnected() {
    const [delayed, setDelayed] = useState(true);

    useEffect(() => {
        setTimeout(() => setDelayed(false), 2000);
    });

    if (delayed) return <></>
    return <>
        <div id={"server-disconnected-component"}>
            <h1>
                <i className={"fa fa-spinner fa-spin"}/>
                Connecting...
            </h1>
        </div>
    </>
}
