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
package org.schedoscope.test

import org.scalatest.{FlatSpec, Matchers}
import org.schedoscope.dsl.Field.v
import org.schedoscope.dsl.Parameter.p
import test.views.{Click, ClickOfEC0101}

class HiveTestFrameworkTest extends SchedoscopeSpec {
  val ec0101Clicks = new Click(p("EC0101"), p("2014"), p("01"), p("01")) with rows {
    set(
      v(id, "event01"),
      v(url, "http://ec0101.com/url1"))
    set(
      v(id, "event02"),
      v(url, "http://ec0101.com/url2"))
    set(
      v(id, "event03"),
      v(url, "http://ec0101.com/url3"))
  }

  val ec0106Clicks = new Click(p("EC0106"), p("2014"), p("01"), p("01")) with rows {
    set(
      v(id, "event04"),
      v(url, "http://ec0106.com/url1"))
    set(
      v(id, "event05"),
      v(url, "http://ec0106.com/url2"))
    set(
      v(id, "event06"),
      v(url, "http://ec0106.com/url3"))
  }

  "Hive test framework" should "execute hive transformations locally" in {
    new ClickOfEC0101(p("2014"), p("01"), p("01")) with test {
      basedOn(ec0101Clicks, ec0106Clicks)
      `then`()
      numRows shouldBe 3
      row(v(id) shouldBe "event01",
        v(url) shouldBe "http://ec0101.com/url1")
      row(v(id) shouldBe "event02",
        v(url) shouldBe "http://ec0101.com/url2")
      row(v(id) shouldBe "event03",
        v(url) shouldBe "http://ec0101.com/url3")
    }
  }
}