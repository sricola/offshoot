# Homebrew formula for offshoot, served straight from this repo as a tap:
#
#   brew tap sricola/offshoot https://github.com/sricola/offshoot
#   brew install offshoot
#
# The explicit URL form of `brew tap` accepts a repo that is not named
# homebrew-*, and brew picks up formulae from the tap's Formula/ directory.
# Source build (depends_on "go" => :build) rather than per-arch bottles:
# it version-bumps by editing url+sha256 only, and mattn/go-sqlite3 compiles
# its bundled SQLite amalgamation via cgo, so no sqlite dependency is needed.
#
# UNTESTED END-TO-END until the repo is public — the url below is only
# fetchable anonymously once it is. sha256 computed from the authenticated
# codeload tarball for the same ref (identical bytes once public).
class Offshoot < Formula
  desc "Branch SQLite like git: fork, checkpoint, rollback, promote"
  homepage "https://github.com/sricola/offshoot"
  url "https://github.com/sricola/offshoot/archive/refs/tags/v0.2.7.tar.gz"
  sha256 "851f89d24f158c87571e49e5004a822d09991935af6992a773702ce46b0bad9e"
  license "Apache-2.0"
  head "https://github.com/sricola/offshoot.git", branch: "main"

  depends_on "go" => :build

  def install
    ENV["CGO_ENABLED"] = "1"
    # release.yml embeds the tag name (v-prefixed) as main.version; match it.
    system "go", "build", *std_go_args(ldflags: "-s -w -X main.version=v#{version}"), "./cmd/offshoot"
  end

  def caveats
    <<~EOS
      `offshoot diff` shells out to sqldiff, which Homebrew ships as its
      own formula (not part of `sqlite`):
        brew install sqldiff
      Everything else, including `offshoot diff --summary`, works without it.
    EOS
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/offshoot version")
    # `brew test` runs in a private tmpdir; init creates ./.offshoot there.
    system bin/"offshoot", "init"
    assert_path_exists testpath/".offshoot"
  end
end
