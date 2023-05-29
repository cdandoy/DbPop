import React from "react";
import {NavLink} from "react-router-dom";

export interface Breadcrumb {
    to?: string;
    label: string;
}

export function Breadcrumbs({breadcrumbs}: {
    breadcrumbs?: Breadcrumb[]
}) {
    if (!breadcrumbs) return <></>;
    return <nav aria-label="breadcrumb">
        <ol className="breadcrumb">
            {breadcrumbs.map(bc => {
                if (bc.to) {
                    return <li className="breadcrumb-item"><NavLink to={bc.to}>{bc.label}</NavLink></li>;
                } else {
                    return <li className="breadcrumb-item active" aria-current="page">{bc.label}</li>
                }
            })}
        </ol>
    </nav>
}