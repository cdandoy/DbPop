import React from "react";
import './PageHeader.scss'

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
        {loading && (
            <div><i className="fa fa-spinner fa-spin"/> Loading...</div>
        )}
        {error && (
            <div className={"alert alert-danger"}>{error}</div>
        )}
    </>
}