import {NavLink} from "react-router-dom";
import SidebarMenu from "../sidebar/SidebarMenu";
import React, {useEffect, useState} from "react";
import {siteApi} from "../../api/siteApi";

export default function Menu() {
    const [hasSource, setHasSource] = useState(false);

    useEffect(() => {   // I should use context or redux for that
        siteApi()
            .then(result => {
                setHasSource(result.data.hasSource);
            })
    }, []);

    return (
        <ul className="menu-links">
            <li className="nav-link">
                <NavLink to={"/"}>
                    <SidebarMenu text="Dashboard" icons="fa fa-home"/>
                </NavLink>
            </li>

            <li className="nav-link">
                <NavLink to={"/datasets"}>
                    <SidebarMenu text="Datasets" icons="fa fa-database"/>
                </NavLink>
            </li>

            {hasSource && (
                <li className="nav-link">
                    <NavLink to={"/download"}>
                        <SidebarMenu text="Download" icons="fa fa-download"/>
                    </NavLink>
                </li>
            )}

            <li className="nav-link">
                <NavLink to={"/vfk"}>
                    <SidebarMenu text="Virtual FKs" icons="fa fa-link"/>
                </NavLink>
            </li>
        </ul>
    )
}