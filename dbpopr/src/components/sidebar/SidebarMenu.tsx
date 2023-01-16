import React from "react";

export default function SidebarMenu({text, icons}:{
    text:string,
    icons:string,
}) {
    return <>
        <i className={`icon ${icons}`}></i>
        <span className="text nav-text">{text}</span>
    </>
}