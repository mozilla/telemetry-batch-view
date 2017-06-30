package com.mozilla.telemetry.metrics

sealed abstract class DerivedScalarDefinition extends ScalarDefinition
case class UintDerivedScalar(keyed: Boolean = false, processes: List[String] = Nil) extends DerivedScalarDefinition
case class LongDerivedScalar(keyed: Boolean = false, processes: List[String] = Nil) extends DerivedScalarDefinition
case class BooleanDerivedScalar(keyed: Boolean = false, processes: List[String] = Nil) extends DerivedScalarDefinition
case class StringDerivedScalar(keyed: Boolean = false, processes: List[String] = Nil) extends DerivedScalarDefinition
