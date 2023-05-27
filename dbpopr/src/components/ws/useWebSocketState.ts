import useWebSocket, {ReadyState} from "react-use-websocket";
import React, {useEffect, useState} from "react";
import {Change, DefaultChanges, targetChanges} from "../../api/changeApi";

const WS_URL = 'ws://localhost:8080/ws/site';

interface Message {
    messageType: 'CODE_CHANGE' | 'HAS_CODE_DIRECTORY';
    hasCodeDirectory?: boolean;
}

export interface WebSocketState {
    codeChanged: number;
    connected: boolean;
    codeChanges: Change[];
    refreshCodeChanges: () => void;
    hasCodeDirectory: boolean;
}

export const WebSocketStateContext = React.createContext<WebSocketState>({
    codeChanged: 0,
    connected: false,
    codeChanges: [],
    refreshCodeChanges: () => {
    },
    hasCodeDirectory: false,
});

export function useWebSocketState(): WebSocketState {
    const [codeChanged, setCodeChanged] = useState(0);
    const [connected, setConnected] = useState(false);
    const [codeChanges, setCodeChanges] = useState<Change[]>(DefaultChanges);
    const [hasCodeDirectory, setHasCodeDirectory] = useState(false);
    const {lastJsonMessage, readyState} = useWebSocket(
        WS_URL,
        {
            shouldReconnect: () => true,
            // reconnectAttempts: 10,
            reconnectInterval: (attemptNumber) =>
                Math.min(Math.pow(2, attemptNumber) * 1000, 10000),
        }
    );

    useEffect(() => {
        if (lastJsonMessage) {
            const message = lastJsonMessage as any as Message;
            if (message.messageType === 'CODE_CHANGE') {
                setCodeChanged(codeChanged + 1);
                refreshCodeChanges();
            }else if (message.messageType === 'HAS_CODE_DIRECTORY'){
                setHasCodeDirectory(!!message.hasCodeDirectory);
            }
        }
    }, [lastJsonMessage]);

    useEffect(() => refreshCodeChanges(), []);

    function refreshCodeChanges() {
        targetChanges()
            .then(result => {
                setCodeChanges(result.data);
            })
    }

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
        codeChanged,
        connected,
        codeChanges,
        refreshCodeChanges: refreshCodeChanges,
        hasCodeDirectory
    }
}