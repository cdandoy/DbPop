import useWebSocket, {ReadyState} from "react-use-websocket";
import React, {useEffect, useState} from "react";
import {Change, DefaultChanges, targetChanges} from "../../api/changeApi";

interface StructuredWebSocketMessage {
    hasSource: boolean;
    hasTarget: boolean;
    hasCode: boolean;
    codeChanges: number;
}

export interface WebSocketState {
    hasSource: boolean;
    hasTarget: boolean;
    codeChanged: number;
    connected: boolean;
    codeChanges: Change[];
    refreshCodeChanges: () => void;
    hasCode: boolean;
}

export const WebSocketStateContext = React.createContext<WebSocketState>({
    hasSource: false,
    hasTarget: false,
    codeChanged: 0,
    connected: false,
    codeChanges: [],
    refreshCodeChanges: () => {
    },
    hasCode: false,
});

function getWebsocketUrl(): string {
    const documentUrl = document.URL;
    const url = new URL(documentUrl);
    const hostname = url.hostname;
    const port = url.port;
    if (hostname === 'localhost' && port === '3000') {
        return 'ws://localhost:8080/ws/site'
    } else {
        return `ws://${hostname}:${port}/ws/site`
    }
}

export function useWebSocketState(): WebSocketState {
    const [hasSource, setHasSource] = useState(false);
    const [hasTarget, setHasTarget] = useState(false);
    const [codeChanged, setCodeChanged] = useState(0);
    const [connected, setConnected] = useState(false);
    const [codeChanges, setCodeChanges] = useState<Change[]>(DefaultChanges);
    const [hasCode, setHasCode] = useState(false);
    const {lastJsonMessage, readyState} = useWebSocket(
        getWebsocketUrl(),
        {
            shouldReconnect: () => true,
            // reconnectAttempts: 10,
            reconnectInterval: (attemptNumber: number) =>
                Math.min(Math.pow(2, attemptNumber) * 1000, 10000),
        }
    );

    useEffect(() => {
        if (lastJsonMessage) {
            const message = lastJsonMessage as any as StructuredWebSocketMessage;
            setHasSource(message.hasSource);
            setHasTarget(message.hasTarget);
            setCodeChanged(message.codeChanges);
            setHasCode(message.hasCode);
        }
    }, [lastJsonMessage]);


    const refreshCodeChanges = () => {
        if (hasCode) {
            targetChanges()
                .then(result => {
                    setCodeChanges(result.data);
                })
        } else {
            setCodeChanges([]);
        }
    };

    useEffect(() => refreshCodeChanges(), [hasCode, codeChanged]);

    const connectionStatus = {
        [ReadyState.CONNECTING]: false,
        [ReadyState.OPEN]: true,
        [ReadyState.CLOSING]: false,
        [ReadyState.CLOSED]: false,
        [ReadyState.UNINSTANTIATED]: false,
    }[readyState];

    useEffect(() => {
        setConnected(connectionStatus);
    }, [readyState]);

    return {
        hasSource,
        hasTarget,
        codeChanged,
        connected,
        codeChanges,
        refreshCodeChanges: refreshCodeChanges,
        hasCode
    }
}
