/*
 * Copyright 2009-2017 java-diff-utils.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dandoy.diff;

import com.github.difflib.DiffUtils;
import com.github.difflib.patch.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

/**
 * This class for generating DiffLines for side-by-sidy view. You can customize
 * the way of generating. For example, show inline diffs on not, ignoring white
 * spaces or/and blank lines and so on. All parameters for generating are
 * optional. If you do not specify them, the class will use the default values.
 * These values are: showInlineDiffs = false; ignoreWhiteSpaces = true;
 * ignoreBlankLines = true; ...
 * For instantiating the DiffLineGenerator you should use its builder. Like in example
 * <code>
 * DiffLineGenerator generator = new DiffLineGenerator.Builder().showInlineDiffs(true).
 * ignoreWhiteSpaces(true).columnWidth(100).build();
 * </code>
 */
public final class DiffLineGenerator {
    private static final BiPredicate<String, String> DEFAULT_EQUALIZER = Object::equals;
    private static final BiPredicate<String, String> IGNORE_WHITESPACE_EQUALIZER = (original, revised) -> adjustWhitespace(original).equals(adjustWhitespace(revised));

    private final BiPredicate<String, String> equalizer;
    private final Function<String, List<String>> inlineDiffSplitter;
    private final boolean reportLinesUnchanged;
    private final Function<String, String> lineNormalizer;
    private final boolean showInlineDiffs;
    private final boolean decompressDeltas;

    DiffLineGenerator(DiffLineGeneratorBuilder builder) {
        showInlineDiffs = builder.showInlineDiffs;
        boolean ignoreWhiteSpaces = builder.ignoreWhiteSpaces;
        inlineDiffSplitter = builder.inlineDiffSplitter;
        decompressDeltas = builder.decompressDeltas;

        if (builder.equalizer != null) {
            equalizer = builder.equalizer;
        } else {
            equalizer = ignoreWhiteSpaces ? IGNORE_WHITESPACE_EQUALIZER : DEFAULT_EQUALIZER;
        }

        reportLinesUnchanged = builder.reportLinesUnchanged;
        lineNormalizer = builder.lineNormalizer;

        Objects.requireNonNull(inlineDiffSplitter);
        Objects.requireNonNull(lineNormalizer);
    }

    /**
     * Splitting lines by character to achieve char by char diff checking.
     */
    public static final Function<String, List<String>> SPLITTER_BY_CHARACTER = line -> {
        List<String> list = new ArrayList<>(line.length());
        for (Character character : line.toCharArray()) {
            list.add(character.toString());
        }
        return list;
    };

    public static final Pattern SPLIT_BY_WORD_PATTERN = Pattern.compile("\\s+|[,.\\[\\](){}/\\\\*+\\-#]");

    /**
     * Splitting lines by word to achieve word by word diff checking.
     */
    public static final Function<String, List<String>> SPLITTER_BY_WORD = DiffLineGenerator::splitStringPreserveDelimiter;
    public static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");

    public static DiffLineGeneratorBuilder create() {
        return new DiffLineGeneratorBuilder();
    }

    private static String adjustWhitespace(String raw) {
        return WHITESPACE_PATTERN.matcher(raw.trim()).replaceAll(" ");
    }

    static List<String> splitStringPreserveDelimiter(String str) {
        List<String> list = new ArrayList<>();
        if (str != null) {
            Matcher matcher = SPLIT_BY_WORD_PATTERN.matcher(str);
            int pos = 0;
            while (matcher.find()) {
                if (pos < matcher.start()) {
                    list.add(str.substring(pos, matcher.start()));
                }
                list.add(matcher.group());
                pos = matcher.end();
            }
            if (pos < str.length()) {
                list.add(str.substring(pos));
            }
        }
        return list;
    }

    /**
     * Generates the DiffLines describing the difference between original and
     * revised texts using the given patch. Useful for displaying side-by-side
     * diff.
     *
     * @param original the original text
     * @param patch    the given patch
     * @return the DiffLines between original and revised texts
     */
    public List<DiffLine> generateDiffLines(final List<String> original, Patch<String> patch) {
        List<DiffLine> diffLines = new ArrayList<>();
        int endPos = 0;
        final List<AbstractDelta<String>> deltaList = patch.getDeltas();

        if (decompressDeltas) {
            for (AbstractDelta<String> originalDelta : deltaList) {
                for (AbstractDelta<String> delta : decompressDeltas(originalDelta)) {
                    endPos = transformDeltaIntoDiffLine(original, endPos, diffLines, delta);
                }
            }
        } else {
            for (AbstractDelta<String> delta : deltaList) {
                endPos = transformDeltaIntoDiffLine(original, endPos, diffLines, delta);
            }
        }

        // Copy the final matching chunk if any.
        for (String line : original.subList(endPos, original.size())) {
            diffLines.add(buildDiffLine(Tag.EQUAL, line, line));
        }
        return diffLines;
    }

    /**
     * Transforms one patch delta into a DiffLine object.
     */
    private int transformDeltaIntoDiffLine(final List<String> original, int endPos, List<DiffLine> diffLines, AbstractDelta<String> delta) {
        Chunk<String> orig = delta.getSource();
        Chunk<String> rev = delta.getTarget();

        for (String line : original.subList(endPos, orig.getPosition())) {
            diffLines.add(buildDiffLine(Tag.EQUAL, line, line));
        }

        switch (delta.getType()) {
            case INSERT -> {
                for (String line : rev.getLines()) {
                    diffLines.add(buildDiffLine(Tag.INSERT, "", line));
                }
            }
            case DELETE -> {
                for (String line : orig.getLines()) {
                    diffLines.add(buildDiffLine(Tag.DELETE, line, ""));
                }
            }
            default -> {
                if (showInlineDiffs) {
                    List<DiffLine> inlineDiffs = generateInlineDiffs(delta);
                    diffLines.addAll(inlineDiffs);
                } else {
                    for (int j = 0; j < Math.max(orig.size(), rev.size()); j++) {
                        diffLines.add(buildDiffLine(Tag.CHANGE,
                                orig.getLines().size() > j ? orig.getLines().get(j) : "",
                                rev.getLines().size() > j ? rev.getLines().get(j) : ""));
                    }
                }
            }
        }

        return orig.last() + 1;
    }

    /**
     * Decompresses ChangeDeltas with different source and target size to a
     * ChangeDelta with same size and a following InsertDelta or DeleteDelta.
     */
    private List<AbstractDelta<String>> decompressDeltas(AbstractDelta<String> delta) {
        if (delta.getType() == DeltaType.CHANGE && delta.getSource().size() != delta.getTarget().size()) {
            List<AbstractDelta<String>> deltas = new ArrayList<>();

            int minSize = Math.min(delta.getSource().size(), delta.getTarget().size());
            Chunk<String> orig = delta.getSource();
            Chunk<String> rev = delta.getTarget();

            deltas.add(new ChangeDelta<>(
                    new Chunk<>(orig.getPosition(), orig.getLines().subList(0, minSize)),
                    new Chunk<>(rev.getPosition(), rev.getLines().subList(0, minSize))));

            if (orig.getLines().size() < rev.getLines().size()) {
                deltas.add(new InsertDelta<>(
                        new Chunk<>(orig.getPosition() + minSize, emptyList()),
                        new Chunk<>(rev.getPosition() + minSize, rev.getLines().subList(minSize, rev.getLines().size()))));
            } else {
                deltas.add(new DeleteDelta<>(
                        new Chunk<>(orig.getPosition() + minSize, orig.getLines().subList(minSize, orig.getLines().size())),
                        new Chunk<>(rev.getPosition() + minSize, emptyList())));
            }
            return deltas;
        }

        return Collections.singletonList(delta);
    }

    private DiffLine buildDiffLine(Tag type, String orgline, String newline) {
        return new DiffLine(
                type,
                List.of(new DiffSegment(orgline)),
                List.of(new DiffSegment(newline))
        );
    }

    List<String> normalizeLines(List<String> list) {
        return reportLinesUnchanged
                ? list
                : list.stream()
                .map(lineNormalizer)
                .collect(toList());
    }

    /**
     * Add the inline diffs for given delta
     *
     * @param delta the given delta
     */
    private List<DiffLine> generateInlineDiffs(AbstractDelta<String> delta) {
        List<String> leftWords = toWords(delta.getSource());
        List<String> rightWords = toWords(delta.getTarget());

        List<AbstractDelta<String>> inlineDeltas = DiffUtils.diff(leftWords, rightWords, equalizer).getDeltas();

        int leftWordsPos = 0;
        int rightWordsPos = 0;
        List<List<DiffSegment>> leftLines = new ArrayList<>();
        List<List<DiffSegment>> rightLines = new ArrayList<>();
        try (LineBuilder leftLineBuilder = new LineBuilder(leftLines)) {
            try (LineBuilder rightLineBuilder = new LineBuilder(rightLines)) {
                for (AbstractDelta<String> inlineDelta : inlineDeltas) {
                    Chunk<String> leftChunk = inlineDelta.getSource();
                    Chunk<String> rightChunk = inlineDelta.getTarget();
                    if (leftWordsPos < leftChunk.getPosition()) {
                        leftLineBuilder.pushWords(Tag.EQUAL, leftWords.subList(leftWordsPos, leftChunk.getPosition()));
                    }
                    if (rightWordsPos < rightChunk.getPosition()) {
                        rightLineBuilder.pushWords(Tag.EQUAL, rightWords.subList(rightWordsPos, rightChunk.getPosition()));
                    }
                    switch (inlineDelta.getType()) {
                        case CHANGE -> {
                            leftLineBuilder.pushWords(Tag.CHANGE, leftChunk.getLines());
                            rightLineBuilder.pushWords(Tag.CHANGE, rightChunk.getLines());
                        }
                        case DELETE -> {
                            leftLineBuilder.pushWords(Tag.INSERT, leftChunk.getLines());
                            rightLineBuilder.pushWords(Tag.DELETE, emptyList());
                        }
                        case INSERT -> {
                            leftLineBuilder.pushWords(Tag.DELETE, emptyList());
                            rightLineBuilder.pushWords(Tag.INSERT, rightChunk.getLines());
                        }
                        case EQUAL -> throw new RuntimeException();
                    }
                    leftWordsPos = leftChunk.getPosition() + leftChunk.size();
                    rightWordsPos = rightChunk.getPosition() + rightChunk.size();
                }
                if (leftWordsPos < leftWords.size()) {
                    leftLineBuilder.pushWords(Tag.EQUAL, leftWords.subList(leftWordsPos, leftWords.size()));
                }
                if (rightWordsPos < rightWords.size()) {
                    rightLineBuilder.pushWords(Tag.EQUAL, rightWords.subList(rightWordsPos, rightWords.size()));
                }
            }
        }

        List<DiffLine> diffLines = new ArrayList<>();
        for (int j = 0; j < Math.max(leftLines.size(), rightLines.size()); j++) {
            List<DiffSegment> leftSegments = leftLines.size() > j ? leftLines.get(j) : emptyList();
            List<DiffSegment> rightSegments = rightLines.size() > j ? rightLines.get(j) : emptyList();

            DiffLine diffLine;
            if (areMostlyChanges(leftSegments, rightSegments)) {
                diffLine = new DiffLine(Tag.CHANGE, mergeChanges(leftSegments), mergeChanges(rightSegments));
            } else {
                diffLine = createDiffLine(leftSegments, rightSegments);
            }

            diffLines.add(diffLine);
        }
        return diffLines;
    }

    private static DiffLine createDiffLine(List<DiffSegment> leftSegments, List<DiffSegment> rightSegments) {
        return new DiffLine(Tag.EQUAL, leftSegments, rightSegments);
    }

    private static List<DiffSegment> mergeChanges(List<DiffSegment> segments) {
        StringBuilder sb = new StringBuilder();
        for (DiffSegment segment : segments) {
            sb.append(segment.text());
        }
        return List.of(new DiffSegment(Tag.CHANGE, sb.toString()));
    }

    private boolean areMostlyChanges(List<DiffSegment> leftSegments, List<DiffSegment> rightSegments) {
        double leftSame = countSame(leftSegments);
        double rightSame = countSame(rightSegments);
        double leftSize = countSize(leftSegments);
        double rightSize = countSize(rightSegments);
        if (leftSize == 0 || rightSize == 0) return false;

        if (leftSame / leftSize < .3) return true;
        if (rightSame / rightSize < .3) return true;

        return false;
    }

    private int countSize(List<DiffSegment> segments) {
        return segments.stream().mapToInt(it -> it.text().length()).sum();
    }

    private static int countSame(List<DiffSegment> segments) {
        return segments.stream().filter(it -> it.tag() == Tag.EQUAL).mapToInt(it -> it.text().length()).sum();
    }


    List<String> toWords(Chunk<String> chunk) {
        List<String> normalized = normalizeLines(chunk.getLines());
        String joined = String.join("\n", normalized);
        return inlineDiffSplitter.apply(joined);
    }
}
