import React from "react";
import './ApiComponent.scss'

export function ApiComponent() {
    return (
        <div className="tab-pane active" id="apidoc-tab-pane" role="tabpanel" aria-labelledby="apidoc-tab" tabIndex={0}>
            <iframe src="/api-docs/"></iframe>
        </div>
    );
}