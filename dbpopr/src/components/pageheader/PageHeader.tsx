import React from "react";
import './PageHeader.scss'
import Spinner from "../Spinner";

export default function PageHeader({title, subtitle, loading, error}: {
    title: string,
    subtitle?: string,
    loading?: boolean,
    error?: string,
}) {
    return <>
        <div className={"pageheader-component"}>
            <header>{title}</header>
            <p><small>{subtitle}</small></p>
        </div>
        {loading && <Spinner/>}
        {error && (
            <div className={"alert alert-danger"}>{error}</div>
        )}
    </>
}