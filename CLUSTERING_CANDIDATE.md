<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Clustering implementation candidate - 2026-11-08

This snapshot is a candidate for a new round of experiments. Persisted Chunk
metadata and coverage-threshold selection remain the core method. Page-M and
Chunk-M share the platform's Page subtraction correction when usable Page
metadata falls below the Page coverage threshold. Unusable or conflicting
metadata still takes the existing raw recovery path.

The original Page correction removes wholly outside subsequences, so a
subsequence crossing a query edge can retain a contribution. This is not a
strict query-intersection refit. K queries retain their existing boundary path.
Historical measurements remain results of their recorded implementation;
they should not be relabeled as measurements of this candidate.

## IoTDB changes

- Shared M subtraction uses the stored M membership (`edIdx`) and the same
  subsequence normalization as Page construction, rather than K cluster IDs.
- Removing a singleton empties the cluster without division by zero; merging
  and evaluation skip zero-weight clusters. Invalid membership/removal fails
  explicitly instead of silently producing negative counts or NaN.
- Corrected Chunk query units preserve the Page correction statistics and
  adapt only their retained span and boundary fragments for Chunk aggregation.
- Original candidate sampling and Page query entry are retained. Overlap/raw
  recovery can still construct temporary statistics.

## Validation

The complete distribution builds from source on JDK 11 using Maven. A focused
`PageMCorrectionTest` checks M memberships, normalized subtraction, singleton
handling and shared Page/Chunk correction. Source-built server jars, without
server class overlays, passed synthetic full/partial, short-range, overlap,
short-tail and multi-page queries. Partial Page-M outputs affected by the
correctness fixes may differ from the preceding version.

Build: `mvn -pl distribution -am package -DskipTests`.
Focused test: `mvn -pl server -am test -Dtest=PageMCorrectionTest
-Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false` (one command).
Native query names are `lsmkshape` / `lsmmshape`, with `level=page|chunk`.
