import React from "react";
import './LoadingOverlay.scss'

export default function LoadingOverlay({active, text}: { active: boolean, text?: string }) {
    if (active) return <div id="overlay">
        <div className={"text"}>
            <i className={"fa fa-spinner fa-spin"}/>
            &nbsp;
            {text || 'Loading'}
        </div>
    </div>
    else return <></>
}