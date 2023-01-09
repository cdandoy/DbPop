import {NavLink} from "react-router-dom";
import React from "react";

export function Header() {
    return (
        <header className="app-header">
            <div className="px-3 py-2 text-bg-dark">
                <div className="container">
                    <div className="d-flex flex-wrap align-items-center justify-content-center justify-content-lg-start">
                        <a href="/" className="d-flex align-items-center my-2 my-lg-0 me-lg-auto text-white text-decoration-none">
                            <h1>DbPop</h1>
                        </a>

                        <ul className="nav col-12 col-lg-auto my-2 justify-content-center my-md-0 text-small">
                            <li>
                                <NavLink to={"/vfk"} className={"nav-link text-white"}>
                                    <i className="nav-icon fa fa-link"></i>
                                    Virtual FKs
                                </NavLink>
                            </li>
                            <li><a className="nav-link text-white" style={{cursor: "default"}}>|</a></li>
                            <li>
                                <a href="https://github.com/cdandoy/DbPop" target="_blank" className="nav-link text-white" rel="noreferrer">
                                    <i className="nav-icon fa-brands fa-docker"></i>
                                    GitHub
                                </a>
                            </li>
                            <li>
                                <a href="https://hub.docker.com/r/cdandoy/dbpop" target="_blank" className="nav-link text-white" rel="noreferrer">
                                    <i className="nav-icon fa-brands fa-github"></i>
                                    Docker
                                </a>
                            </li>
                            <li>
                                <a href="/api-docs/" target="_blank" className="nav-link text-white">
                                    <i className="nav-icon fa fa-book"></i>
                                    API
                                </a>
                            </li>
                        </ul>
                    </div>
                </div>
            </div>
        </header>
    );
}