import useWebSocket, {ReadyState} from "react-use-websocket";
import React, {useEffect, useState} from "react";
import {Change, DefaultChanges, targetChanges} from "../../api/changeApi";

const WS_URL = 'ws://localhost:8080/ws/site';

interface Message {
    messageType: string
}

export interface WebSocketState {
    codeChanged: number;
    connected: boolean;
    codeChanges: Change[];
    refreshCodeChanges:() => void;
}

export const WebSocketStateContext = React.createContext<WebSocketState>({
    codeChanged: 0,
    connected: false,
    codeChanges: [],
    refreshCodeChanges: () => {
    }
});

export function useWebSocketState(): WebSocketState {
    const [codeChanged, setCodeChanged] = useState(0);
    const [connected, setConnected] = useState(false);
    const [codeChanges, setCodeChanges] = useState<Change[]>(DefaultChanges);
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
    }
}