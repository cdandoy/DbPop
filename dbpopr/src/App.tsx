import './App.scss';
import 'react-bootstrap-typeahead/css/Typeahead.css';
import React from 'react';
import {HashRouter, Route, Routes} from "react-router-dom";
import DownloadDatasetDetailsComponent from "./download/DownloadDatasetDetailsComponent";
import DownloadDatasetsComponent from "./download/DownloadDatasetsComponent"
import DownloadAdd from "./download/DownloadAdd";

function Header() {
    return (
        <header>
            <div className="px-3 py-2 text-bg-dark">
                <div className="container">
                    <div className="d-flex flex-wrap align-items-center justify-content-center justify-content-lg-start">
                        <a href="/" className="d-flex align-items-center my-2 my-lg-0 me-lg-auto text-white text-decoration-none">
                            <h1>DbPop</h1>
                        </a>

                        <ul className="nav col-12 col-lg-auto my-2 justify-content-center my-md-0 text-small">
                            <li>
                                <a href="https://github.com/cdandoy/DbPop" className="nav-link text-white">
                                    <i className="fa-brands fa-docker"></i>
                                    GitHub
                                </a>
                            </li>
                            <li>
                                <a href="https://hub.docker.com/repository/docker/cdandoy/dbpop" className="nav-link text-white">
                                    <i className="fa-brands fa-github"></i>
                                    Docker
                                </a>
                            </li>
                        </ul>
                    </div>
                </div>
            </div>
        </header>
    );
}


export default function App() {
    return (
        <>
            <Header/>
            <div className="container">
                <HashRouter>
                    <div className="text-center m-5">
                        <h1>Welcome to DbPop</h1>
                        <p className="lead">The easiest way to populate your development database.</p>
                    </div>
                    <div id="download-component" className="row">
                        <Routes>
                            <Route path="/dataset/:dataset" element=<DownloadDatasetDetailsComponent/>/>
                            <Route path="/add/:datasetName" element=<DownloadAdd/>/>
                            <Route path="/" element=<DownloadDatasetsComponent/>/>
                        </Routes>
                    </div>
                </HashRouter>
            </div>
        </>
    );
}
