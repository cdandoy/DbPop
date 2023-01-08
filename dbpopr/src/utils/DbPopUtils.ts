export function toHumanReadableSize(bytes: number) {
    let unit = bytes === 1 ? 'byte' : 'bytes';
    let size = bytes;
    if (size > 1024) {
        size = Math.round(size / 1024);
        unit = "Kb";
    }
    if (size > 1024) {
        size = Math.round(size / 1024);
        unit = "Mb";
    }
    return {
        size: size,
        unit: unit,
        text: `${size.toLocaleString()} ${unit}`
    }
}