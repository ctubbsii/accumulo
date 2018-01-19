/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.api.data;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AbstractIDTest {

  private static class One extends AbstractId {
    private static final long serialVersionUID = 1L;

    public One(String s) {
      super(s);
    }
  }

  private static class Two extends AbstractId {
    private static final long serialVersionUID = 1L;

    public Two(String s) {
      super(s);
    }
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private One one;
  private Two two;

  @Before
  public void setUp() {
    one = new One("one");
    two = new Two("two");
  }

  /**
   * Test method for {@link org.apache.accumulo.api.data.AbstractId#AbstractId(String)}.
   */
  @Test
  public void testAbstractID() throws Exception {
    one = new One("notNull"); // should pass

    // Use reflection to get past findbugs check for null parameter, which is what I'm trying to test;
    // It's good to know that findbugs has my back, but I want to make sure that my sanity check works,
    // in case users are running findbugs themselves
    Class<One> cl = One.class;
    Constructor<One> constr = cl.getConstructor(String.class);

    // because we're using reflection, we have to expect the NullPointerException to be wrapped
    expectedException.expect(InvocationTargetException.class);
    expectedException.expectCause(CoreMatchers.isA(NullPointerException.class));
    one = constr.newInstance((String) null); // should throw
  }

  /**
   * Test method for {@link org.apache.accumulo.api.data.AbstractId#canonicalID()}.
   */
  @Test
  public void testCanonicalID() {
    Assert.assertEquals("one", one.canonicalID());
    Assert.assertEquals("two", two.canonicalID());

    Assert.assertNotEquals("two", one.canonicalID());
    Assert.assertNotEquals("one", two.canonicalID());
  }

  /**
   * Test method for {@link org.apache.accumulo.api.data.AbstractId#equals(java.lang.Object)}.
   */
  @Test
  public void testEqualsObject() {
    Assert.assertEquals(one, one);
    Assert.assertEquals(new One("one"), one);
    Assert.assertEquals(one, new One("one"));

    Assert.assertNotEquals(one, two);
    Assert.assertNotEquals(one, new Two("one"));
    Assert.assertNotEquals(two, new One("two"));

    Assert.assertSame(one, one);
    Assert.assertNotSame(new One("one"), one);
  }

  /**
   * Test method for {@link org.apache.accumulo.api.data.AbstractId#hashCode()}.
   */
  @Test
  public void testHashCode() {
    Assert.assertEquals(one.hashCode(), new One("one").hashCode());
    Assert.assertEquals(two.hashCode(), new Two("two").hashCode());

    Assert.assertNotEquals(one.hashCode(), new One("two").hashCode());
    Assert.assertNotEquals(two.hashCode(), new Two("one").hashCode());
  }

  /**
   * Test method for {@link org.apache.accumulo.api.data.AbstractId#toString()}.
   */
  @Test
  public void testToString() {
    Assert.assertEquals("one", one.toString());
    Assert.assertEquals("two", two.toString());

    Assert.assertNotEquals("two", one.canonicalID());
    Assert.assertNotEquals("one", two.canonicalID());
  }

}
