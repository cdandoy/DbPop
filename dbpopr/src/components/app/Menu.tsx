import {NavLink} from "react-router-dom";
import SidebarMenu from "../sidebar/SidebarMenu";
import React, {useContext} from "react";
import {WebSocketStateContext} from "../ws/useWebSocketState";

export default function Menu() {
    const siteStatus = useContext(WebSocketStateContext);

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

            {(siteStatus.hasSource || siteStatus.hasTarget) &&
                <li className="nav-link">
                    {siteStatus.hasSource && siteStatus.hasTarget && <NavLink to={"/tools"}><SidebarMenu text="Tools" icons="fa fa-hammer"/></NavLink>}
                    {siteStatus.hasSource && !siteStatus.hasTarget && <NavLink to={"/tools/source"}><SidebarMenu text="Tools" icons="fa fa-hammer"/></NavLink>}
                    {!siteStatus.hasSource && siteStatus.hasTarget && <NavLink to={"/tools/target"}><SidebarMenu text="Tools" icons="fa fa-hammer"/></NavLink>}
                </li>
            }

            {siteStatus.hasSource && (
                <li className="nav-link">
                    <NavLink to={"/vfk"}>
                        <SidebarMenu text="Virtual FKs" icons="fa fa-link"/>
                    </NavLink>
                </li>
            )}

            <li className="nav-link">
                <NavLink to={"/settings"}><SidebarMenu text="Settings" icons="fa fa-cogs"/></NavLink>
            </li>
        </ul>
    )
}