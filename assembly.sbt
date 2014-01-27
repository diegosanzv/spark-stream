import AssemblyKeys._ 
import sbtassembly.Plugin._
import sbt.Package.ManifestAttributes

assemblySettings

test in assembly := {}

mergeStrategy in assembly <<= (mergeStrategy in assembly) {
	case old => {
		case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
		case m if m.matches(".*log4j.properties") => MergeStrategy.discard
		case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
		case PathList("javax", "xml", xs @ _*) => MergeStrategy.first
	    case PathList("org", "w3c", "dom", "TypeInfo.class") => MergeStrategy.first
	    case x => MergeStrategy.first
	}
}
