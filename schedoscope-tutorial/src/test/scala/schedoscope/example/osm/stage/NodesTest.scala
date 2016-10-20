/**
  * Copyright 2015 Otto (GmbH & Co KG)
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
package schedoscope.example.osm.stage

import org.schedoscope.test.{SchedoscopeSpec, test}

class NodesTest extends SchedoscopeSpec {

  val nodes = putViewUnderTest(new Nodes with test)

  import nodes._

  "stage.Nodes" should "load correctly from classpath" in {
    numRows shouldBe 10
  }

  "stage.Nodes" should "load the first node" in {
    row(v(id) shouldBe 122317,
      v(tstamp) shouldBe "2014-10-17T13:49:26Z",
      v(version) shouldBe 7,
      v(userId) shouldBe 50299,
      v(longitude) shouldBe 10.0232716,
      v(latitude) shouldBe 53.5282633)
  }

  it should "load the second node" in {
    startWithRow(1)
    row(v(id) shouldBe 122318,
      v(tstamp) shouldBe "2014-10-17T13:49:26Z",
      v(version) shouldBe 6,
      v(userId) shouldBe 50299,
      v(longitude) shouldBe 10.0243161,
      v(latitude) shouldBe 53.5297589)
  }

  it should "load the third node" in {
    startWithRow(2)
    row(v(id) shouldBe 122320,
      v(tstamp) shouldBe "2013-12-20T07:43:33Z",
      v(version) shouldBe 4,
      v(userId) shouldBe 51991,
      v(longitude) shouldBe 10.0293114,
      v(latitude) shouldBe 53.5351834)
  }
}