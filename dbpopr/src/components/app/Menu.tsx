import {NavLink} from "react-router-dom";
import SidebarMenu from "../sidebar/SidebarMenu";
import React from "react";

export default function Menu() {
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
                <NavLink to={"/download"}>
                    <SidebarMenu text="Download" icons="fa fa-download"/>
                </NavLink>
            </li>

            <li className="nav-link">
                <NavLink to={"/code"}>
                    <SidebarMenu text="Code" icons="fa fa-code"/>
                </NavLink>
            </li>

            <li className="nav-link">
                <NavLink to={"/vfk"}>
                    <SidebarMenu text="Virtual FKs" icons="fa fa-link"/>
                </NavLink>
            </li>
        </ul>
    )
}