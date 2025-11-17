/*
 * Copyright © 2015 Integrated Knowledge Management (support@ikm.dev)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.ikm.tinkar.entity.util;

import dev.ikm.tinkar.common.util.time.Stopwatch;
import dev.ikm.tinkar.component.FieldDataType;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ObjIntConsumer;

public abstract class EntityProcessor implements ObjIntConsumer<byte[]> {

    LongAdder totalCount = new LongAdder();
    LongAdder conceptCount = new LongAdder();
    LongAdder semanticCount = new LongAdder();
    LongAdder patternCount = new LongAdder();
    LongAdder stampCount = new LongAdder();
    LongAdder other = new LongAdder();
    Stopwatch stopwatch = new Stopwatch();

    @Override
    public void accept(byte[] bytes, int value) {
        // bytes starts with number of arrays (int = 4 bytes), then size of first array (int = 4 bytes), then entity format version then type token, -1 since index starts at 0...
        FieldDataType componentType = FieldDataType.fromToken(bytes[9]);
        totalCount.increment();
        switch (componentType) {
            case PATTERN_CHRONOLOGY -> patternCount.increment();
            case CONCEPT_CHRONOLOGY -> conceptCount.increment();
            case SEMANTIC_CHRONOLOGY -> semanticCount.increment();
            case STAMP -> stampCount.increment();
            default -> other.increment();
        }
        processBytesForType(componentType, bytes);
    }

    public abstract void processBytesForType(FieldDataType componentType, byte[] bytes);

    public void finish() {
        this.stopwatch.end();
    }

    public String report() {
        finish();
        StringBuilder sb = new StringBuilder();
        sb.append("Finished: ").append(this.getClass().getSimpleName());
        sb.append("\nDuration: ").append(stopwatch.durationString());
        sb.append("\nAverage realization time: ").append(stopwatch.averageDurationForElementString((int) totalCount.sum()));
        if (conceptCount.sum() > 0) {
            sb.append("\nConcepts: ").append(conceptCount);
        }
        if (semanticCount.sum() > 0) {
            sb.append(" Semantics: ").append(semanticCount);
        }
        if (patternCount.sum() > 0) {
            sb.append(" Patterns: ").append(patternCount);
        }
        if (stampCount.sum() > 0) {
            sb.append(" Stamps: ").append(stampCount);
        }
        if (other.sum() > 0) {
            sb.append(" Others: ").append(other);
        }
        sb.append("\nTotal: ").append(totalCount).append("\n\n");

        return sb.toString();
    }
}
