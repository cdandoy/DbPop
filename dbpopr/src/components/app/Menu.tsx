import {NavLink} from "react-router-dom";
import SidebarMenu from "../sidebar/SidebarMenu";
import React, {useContext} from "react";
import {SiteContext} from "./App";
import {useCodeChanges} from "../codechanges/CodeChanges";

export default function Menu() {
    const siteResponse = useContext(SiteContext);
    const {changes} = useCodeChanges();

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

            <li className="nav-link">
                <NavLink to={"/codechanges"}>
                    <SidebarMenu text="Code Changes" icons="fa fa-code"/>
                    {changes && changes.length > 0 &&
                        <span style={{position: "absolute", right: "20px"}} title={"Code Change Detected"}>
                            <i className={"fa fa-circle"} style={{color: "#ffb000"}}></i>
                        </span>
                    }
                </NavLink>
            </li>

            <li className="nav-link">
                <NavLink to={"/tools"}>
                    <SidebarMenu text="Tools" icons="fa fa-cogs"/>
                </NavLink>
            </li>

            {siteResponse.hasSource && (
                <li className="nav-link">
                    <NavLink to={"/vfk"}>
                        <SidebarMenu text="Virtual FKs" icons="fa fa-link"/>
                    </NavLink>
                </li>
            )}
        </ul>
    )
}