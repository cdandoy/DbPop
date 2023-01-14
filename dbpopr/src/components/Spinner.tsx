import React from "react";

export default function Spinner({text}: { text?: string }) {
    return <div><i className={"fa fa-spinner fa-spin"}/> {text || 'Loading'}...</div>;
}