import {NavLink} from "react-router-dom";
import React from "react";

export function Section({title, description, to, img, disabled}: {
    title: string;
    description: string | JSX.Element;
    to: string;
    img?: any;
    disabled?: boolean;
}) {
    return <>
        <div className={"code-component-section row mt-5"}>
            <div className={"col-8"}>
                <h3>{title}</h3>
                <p>{description}</p>
                <div className={"ms-5 button-bar"}>
                    <NavLink to={to}>
                        <button className={"btn btn-primary"} disabled={disabled}>
                            Select
                        </button>
                    </NavLink>
                </div>
            </div>
            <div className={"col-4"}>
                <img src={img} alt={title}/>
            </div>
        </div>
    </>
}