/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractLocalOperationTest {

    private AbstractLocalOperation operation = new AbstractLocalOperation() {
        @Override
        public void run() throws Exception {
        }
    };

    @Test(expected = UnsupportedOperationException.class)
    public void testWriteInternal() throws Exception {
        operation.writeInternal(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testReadInternal() throws Exception {
        operation.readInternal(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetFactoryId() {
        operation.getFactoryId();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetId() {
        operation.getId();
    }
}