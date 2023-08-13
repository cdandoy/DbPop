import React, {useContext, useState} from "react";
import './Sidebar.scss'
import {WebSocketStateContext} from "../ws/useWebSocketState";

function databaseStatuses(hasSource: boolean, sourceErrorMessage: string | undefined, hasTarget: boolean, targetErrorMessage: string | undefined) {
    function databaseLight(has: boolean, errorMessage: string | undefined, name: string) {
        return <i
            className={"fa fa-circle fa-xs"}
            style={{color: errorMessage ? 'orangered' : has ? 'green' : 'lightgray'}}
            title={`${name}: ${errorMessage ? errorMessage : has ? 'Connected' : 'Undefined'}`}
        />;
    }

    return <>
        <div>
            <span className="db-status">
                {databaseLight(hasSource, sourceErrorMessage, "Source Database")}
                &nbsp;
                {databaseLight(hasTarget, targetErrorMessage, "Target Database")}
            </span>
        </div>
    </>
}

// See https://www.codinglabweb.com/2022/01/sidebar-menu-in-html-css-javascript.html
export default function Sidebar({title1, title2, search, children, menu, bottomMenu}: {
    title1: string,
    title2: string,
    children: any,
    search?: ((s: string) => void),
    menu?: React.ReactNode;
    bottomMenu?: React.ReactNode;
}) {
    const siteStatus = useContext(WebSocketStateContext);
    const [closed, setClosed] = useState(false);

    return <>
        <div id={"sidebar-root"}>
            <nav className={`sidebar ${closed ? 'close' : ''}`}>
                <header>
                    {closed ?
                        databaseStatuses(siteStatus.hasSource, siteStatus.sourceErrorMessage, siteStatus.hasTarget, siteStatus.targetErrorMessage)
                        :
                        <div className="image-text">
                        <span className="image">
                            {/*<img src="logo.png" alt=""/>*/}
                        </span>
                            <div className="text logo-text">
                                <span className="title1">{title1}</span>
                                <span className="title2">{title2}</span>
                                {databaseStatuses(siteStatus.hasSource, siteStatus.sourceErrorMessage, siteStatus.hasTarget, siteStatus.targetErrorMessage)}
                            </div>
                        </div>
                    }

                    <button className="toggle" onClick={() => setClosed(!closed)}>
                        <i className='fa fa-chevron-right'/>
                    </button>
                </header>

                <div className="menu-bar">
                    <div className="menu">

                        {search && (
                            <li className="search-box">
                                <i className='fa fa-search icon'></i>
                                <input type="text" placeholder="Search..."/>
                            </li>
                        )}

                        {menu}

                    </div>

                    {bottomMenu && (
                        <div className="bottom-content">
                            {bottomMenu}
                        </div>
                    )}


                </div>

            </nav>

            <section className="home">
                {children}
            </section>
        </div>
    </>
}