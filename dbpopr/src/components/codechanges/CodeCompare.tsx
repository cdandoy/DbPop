import React, {useContext, useEffect, useState} from "react";
import axios, {AxiosResponse} from "axios";
import {ObjectIdentifier} from "../../models/ObjectIdentifier";
import "./CodeCompare.scss"
import {WebSocketStateContext} from "../ws/useWebSocketState";

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
    return <>
        {segments.map(segment => <span className={"code-compare-segment-" + segment.tag}>{segment.text}</span>)}
    </>
}

export function CodeCompare() {
    const messageState = useContext(WebSocketStateContext);
    const [diffLines, setDiffLines] = useState<DiffLine[] | undefined>()

    useEffect(() => {
        axios.post<ObjectIdentifier, AxiosResponse<DiffLine[]>>(`/code/target/diff/`, {
            type: "SQL_STORED_PROCEDURE",
            tableName: {
                catalog: "master",
                schema: "dbo",
                table: "GetInvoices"
            }
        }).then(result => {
            setDiffLines(result.data);
        });
    }, [messageState.codeChanged]);

    if (!diffLines) return <div>Loading...</div>
    return <>
        <div id={"code-compare-component"}>
            <div className={"code-compare-left"}>
                {diffLines?.map(diffLine => <div className={"code-compare-line-" + diffLine.tag}>
                    <Segments segments={diffLine.leftSegments}/>&nbsp;
                </div>)}
            </div>
            <div className={"code-compare-right"}>
                {diffLines?.map(diffLine => <div className={"code-compare-line-" + diffLine.tag}>
                    <Segments segments={diffLine.rightSegments}/>&nbsp;
                </div>)}
            </div>
        </div>
    </>
}