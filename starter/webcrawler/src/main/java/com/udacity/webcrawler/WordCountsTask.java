package com.udacity.webcrawler;

import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class WordCountsTask extends RecursiveAction {
    private final int maxDepth;
    private final Clock clock;
    private final Instant deadline;
    private final String url;
    private final PageParserFactory parserFactory;
    private final ConcurrentHashMap<String,Integer> counts;//used Map in ref
    private final ConcurrentSkipListSet<String> visitedUrls;
    private final List<Pattern> ignoredUrls;

    public WordCountsTask(int maxDepth, Clock clock, Instant deadline, String url, PageParserFactory parserFactory, ConcurrentHashMap<String, Integer> counts, ConcurrentSkipListSet<String> visitedUrls, List<Pattern> ignoredUrls) {
        this.maxDepth = maxDepth;
        this.clock = clock;
        this.deadline = deadline;
        this.url = url;
        this.counts = counts;
        this.visitedUrls = visitedUrls;
        this.ignoredUrls = ignoredUrls;
        this.parserFactory = parserFactory;
    }


    @Override
    protected void compute() {
        if(maxDepth == 0 || clock.instant().isAfter(deadline)){
            return;
        }
        for (final Pattern pattern: ignoredUrls){
            if (pattern.matcher(url).matches()){
                return;
            }
        }
        if (visitedUrls.contains(url)){return;}
        visitedUrls.add(url);

        PageParser.Result result = parserFactory.get(url).parse();
        for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
            counts.compute(e.getKey(), (k, v) -> (v == null) ? e.getValue(): e.getValue() + counts.get(e.getKey()));
        }

        final List<WordCountsTask> subTasks = result.getLinks()
                .stream()
                .map(link -> new WordCountsTask.Builder()
                        .setMaxDepth(maxDepth-1)
                        .setClock(clock)
                        .setCounts(counts)
                        .setDeadline(deadline)
                        .setUrl(url)
                        .setIgnoredUrls(ignoredUrls)
                        .setParserFactory(parserFactory)
                        .setVisitedUrls(visitedUrls)
                        .build())
                .collect(Collectors.toList());
        invokeAll(subTasks);

    }
    public static final class Builder {
        private int maxDepth;
        private Clock clock;
        private Instant deadline;
        private String url;
        private PageParserFactory parserFactory;
        private ConcurrentHashMap<String, Integer> counts;
        private ConcurrentSkipListSet<String> visitedUrls;
        private List<Pattern> ignoredUrls;

        public Builder setMaxDepth(final int maxDepth) {
            this.maxDepth = maxDepth;
            return this;
        }

        public Builder setClock(final Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder setDeadline(final Instant deadline) {
            this.deadline = deadline;
            return this;
        }

        public Builder setUrl(final String url) {
            this.url = url;
            return this;
        }

        public Builder setParserFactory(final PageParserFactory parserFactory) {
            this.parserFactory = parserFactory;
            return this;
        }

        public Builder setCounts(final ConcurrentHashMap<String, Integer> counts) {
            this.counts = counts;
            return this;
        }

        public Builder setVisitedUrls(final ConcurrentSkipListSet<String> visitedUrls) {
            this.visitedUrls = visitedUrls;
            return this;
        }

        public Builder setIgnoredUrls(final List<Pattern> ignoredUrls) {
            this.ignoredUrls = ignoredUrls;
            return this;
        }

        public WordCountsTask build() {
            return new WordCountsTask(maxDepth, clock, deadline, url,
                    parserFactory, counts, visitedUrls, ignoredUrls);
        }
    }
}
