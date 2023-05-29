import React, {useContext, useEffect, useState} from "react";
import axios, {AxiosResponse} from "axios";
import {ObjectIdentifier} from "../../models/ObjectIdentifier";
import "./CodeCompare.scss"
import {WebSocketStateContext} from "../ws/useWebSocketState";
import {NavLink, useLocation, useNavigate} from "react-router-dom";
import PageHeader from "../pageheader/PageHeader";

interface CodeDiff {
    diffLines: DiffLine[];
    leftName: string;
    rightName: string;
}

interface DiffLine {
    tag: string;
    leftSegments: DiffSegment[];
    rightSegments: DiffSegment[];
}

interface DiffSegment {
    tag: string;
    text: string;
}

function Segments({segments}: { segments: DiffSegment[] }) {
    let segmentNo = 0;
    return <>
        {segments.map(segment => <span key={`seg-${segmentNo++}`} className={"code-compare-segment-" + segment.tag}>{segment.text}</span>)}
    </>
}

function Tool() {
    return <div>
        <NavLink to={"/codechanges"}>
            <span title={"Esc"}><i className={"fa fa-arrow-left"}/> Back</span>
        </NavLink>
    </div>
}

export function CodeCompare() {
    let location = useLocation();
    let objectIdentifier: ObjectIdentifier = location.state;
    let navigate = useNavigate();
    const messageState = useContext(WebSocketStateContext);
    const [codeDiff, setCodeDiff] = useState<CodeDiff | undefined>()

    useEffect(() => {
        axios.post<ObjectIdentifier, AxiosResponse<CodeDiff>>(`/code/target/diff/`, objectIdentifier)
            .then(result => {
                setCodeDiff(result.data);
            });
    }, [messageState.codeChanged]);

    useEffect(() => {
        const ESCAPE_KEYS = ['27', 'Escape'];

        function handler({key}: { key: any }) {
            if (ESCAPE_KEYS.includes(key)) {
                navigate(-1);
            }
        }

        document.addEventListener('keydown', handler);
        return () => {
            document.removeEventListener('keydown', handler);
        }
    }, []);

    if (!codeDiff) return <div>Loading...</div>
    let lineNo = 0;
    return <div className={"container"}>
        <PageHeader title={codeDiff.rightName} tool={<Tool/>}/>
        <div id={"code-compare-component"}>
            <div className={"code-compare-col"}>
                <h5>File</h5>
                <div className={"code-compare-left"}>
                    {codeDiff.diffLines?.map(diffLine => <div key={`left-${lineNo++}`} className={"code-compare-line-" + diffLine.tag}>
                        <Segments segments={diffLine.leftSegments}/>&nbsp;
                    </div>)}
                </div>
            </div>
            <div className={"code-compare-col"}>
                <h5>Database</h5>
                <div className={"code-compare-right"}>
                    {codeDiff.diffLines?.map(diffLine => <div key={`right-${lineNo++}`} className={"code-compare-line-" + diffLine.tag}>
                        <Segments segments={diffLine.rightSegments}/>&nbsp;
                    </div>)}
                </div>
            </div>
        </div>
    </div>
}