import './App.scss';
import React, {useEffect, useState} from 'react';
import PopulateComponent from "./populate/PopulateComponent";
import {HashRouter, Navigate, Route, Routes} from "react-router-dom";
import Download from "./download/Download";

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

function AppSwitch() {
    const [error, setError] = useState(null);
    const [isLoaded, setIsLoaded] = useState(false);
    const [mode, setMode] = useState(null);

    useEffect(() => {
        fetch("/site")
            .then(res => res.json())
            .then(
                (result) => {
                    setIsLoaded(true);
                    setMode(result.mode);
                },
                (error) => {
                    setIsLoaded(true);
                    setError(error);
                }
            )
    }, []);

    if (error) {
        return <div>Error: {error}</div>;
    } else if (!isLoaded) {
        return <div className="text-center">Loading...</div>;
    } else {
        return (
            <Routes>
                {mode === "populate" && <Route path="populate/*" element={<PopulateComponent/>}/>}
                {mode === "download" && <Route path="download/*" element={<Download/>}/>}
                {mode === null && <Route path="/" element={<div>Loading</div>}/>}
                {mode === "populate" && <Route path="/" element={<Navigate to="/populate/datasets" replace/>}/>}
                {mode === "download" && <Route path="/" element={<Navigate to="/download" replace/>}/>}
            </Routes>
        )
    }
}

function App() {
    return (
        <>
            <Header/>
            <div className="container">
                <HashRouter>
                    <div className="text-center m-5">
                        <h1>Welcome to DbPop</h1>
                        <p className="lead">The easiest way to populate your development database.</p>
                    </div>
                    <div id="app-switch">
                        <AppSwitch/>
                    </div>
                </HashRouter>
            </div>
        </>
    );
}


export default App;
