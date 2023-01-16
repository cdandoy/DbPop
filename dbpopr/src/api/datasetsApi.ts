import axios from "axios";

export default function datasetsApi() {
    return axios.get<string[]>('/datasets');
}