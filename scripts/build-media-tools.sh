#!/usr/bin/env bash
# Native static FFmpeg/FFprobe build. Run from the repository after prepare-ffmpeg.py.
set -euo pipefail

platform="${1:?usage: build-media-tools.sh PLATFORM [BUNDLE_DIR] [CACHE_DIR]}"
bundle="${2:-bundle}"
cache="${3:-.release-cache}"
case "$platform" in
  windows-x64|windows-arm64|linux-x64|linux-arm64|macos-x64|macos-arm64) ;;
  *) echo "Unsupported platform: $platform" >&2; exit 1 ;;
esac

mkdir -p "$bundle" "$cache/build"
bundle="$(cd "$bundle" && pwd)"
cache="$(cd "$cache" && pwd)"
prefix="$cache/build/prefix"
jobs="${MEDIA_BUILD_JOBS:-4}"
mkdir -p "$prefix"
export PKG_CONFIG_PATH="$prefix/lib/pkgconfig"
export PKG_CONFIG_LIBDIR="$prefix/lib/pkgconfig"
export GIT_CEILING_DIRECTORIES="$cache/source"
compiler="cc"
cflags="-O2"
ldflags=""
suffix=""
tls_flag="--enable-openssl"
x264_flags=(--enable-static --disable-cli --enable-pic)
ffmpeg_flags=(--disable-shared --enable-static --enable-gpl --enable-version3 --enable-libx264 --enable-libdav1d --enable-libass --disable-autodetect --disable-debug --disable-doc --disable-ffplay --pkg-config-flags=--static)
meson_flags=(--default-library=static --buildtype=release --wrap-mode=nodownload -Denable_tools=false -Denable_tests=false -Denable_examples=false -Denable_docs=false -Dxxhash_muxer=disabled)

case "$platform" in
  windows-*)
    suffix=".exe"
    compiler="clang"
    export CC=clang CXX=clang++ AR=llvm-ar RANLIB=llvm-ranlib
    ldflags="-static -static-libgcc -static-libstdc++"
    tls_flag="--enable-schannel"
    ffmpeg_flags+=(--target-os=mingw32 --cc=clang --cxx=clang++ --ld=clang --ar=llvm-ar --ranlib=llvm-ranlib --nm=llvm-nm)
    # x264's Windows ARM assembler requires a distinct MSVC assembly pipeline.
    # The portable C implementation keeps this native Clang build self-contained.
    if [[ "$platform" == "windows-arm64" ]]; then
      x264_flags+=(--host=aarch64-w64-mingw32 --disable-asm)
      ffmpeg_flags+=(--arch=aarch64)
    else
      x264_flags+=(--host=x86_64-w64-mingw32)
      ffmpeg_flags+=(--arch=x86_64)
    fi
    ;;
  macos-*)
    export MACOSX_DEPLOYMENT_TARGET=13.0
    cflags="$cflags -mmacosx-version-min=13.0"
    ldflags="-mmacosx-version-min=13.0"
    tls_flag="--enable-securetransport"
    ;;
  linux-*)
    ldflags="-static-libgcc -static-libstdc++"
    ;;
esac
compiler_prefix="$prefix"
if [[ "$platform" == windows-* ]]; then compiler_prefix="$(cygpath -m "$prefix")"; fi
cflags="$cflags -I\"$compiler_prefix/include\""
ldflags="$ldflags -L\"$compiler_prefix/lib\""
export CFLAGS="$cflags" CXXFLAGS="$cflags" LDFLAGS="$ldflags"

build_meson() {
  local name="$1"
  shift
  local source=("$cache/source/$name"/*)
  local reconfigure=()
  if [[ -f "$cache/build/$name/meson-private/coredata.dat" ]]; then reconfigure=(--wipe); fi
  meson setup "$cache/build/$name" "${source[0]}" "--prefix=$prefix" --libdir=lib --default-library=static --buildtype=release --wrap-mode=nodownload -Dauto_features=disabled "${reconfigure[@]}" "$@"
  meson compile -C "$cache/build/$name" -j "$jobs"
  meson install -C "$cache/build/$name"
}

if [[ "$platform" == linux-* ]]; then
  openssl_source=("$cache"/source/openssl/*)
  (
    cd "${openssl_source[0]}"
    ./Configure no-shared no-module no-tests no-docs "--prefix=$prefix" --libdir=lib --openssldir=/etc/ssl
    make -j"$jobs"
    make install_sw
  )
fi

build_meson freetype -Dharfbuzz=disabled -Dzlib=disabled -Dbrotli=disabled -Dbzip2=disabled -Dpng=disabled -Dtests=disabled
build_meson fribidi -Ddocs=false -Dbin=false -Dtests=false
build_meson harfbuzz -Dfreetype=enabled -Dglib=disabled -Dgobject=disabled -Dcairo=disabled -Dicu=disabled -Dgraphite2=disabled -Dtests=disabled -Dutilities=disabled -Ddocs=disabled -Dintrospection=disabled -Dsubset=disabled -Draster=disabled -Dvector=disabled -Dgpu=disabled -Dwith_libstdcxx=false

if [[ "$platform" == windows-* ]]; then
  iconv_source=("$cache"/source/libiconv/*)
  (
    cd "${iconv_source[0]}"
    ./configure "--prefix=$prefix" --enable-static --disable-shared --disable-nls --disable-rpath
    make -j"$jobs"
    make install
  )
  cat > "$prefix/lib/pkgconfig/iconv.pc" <<EOF
prefix=$prefix
libdir=\${prefix}/lib
includedir=\${prefix}/include
Name: iconv
Description: Source-built static GNU libiconv
Version: 1.19.1
Libs: -L\${libdir} -liconv
Cflags: -I\${includedir} -DLIBICONV_STATIC
EOF
fi

ass_flags=(-Dtest=disabled -Dcompare=disabled -Dprofile=disabled -Dfuzz=disabled -Dcheckasm=disabled -Dlibunibreak=disabled)
case "$platform" in
  linux-*)
    expat_source=("$cache"/source/expat/*)
    cmake -S "${expat_source[0]}/expat" -B "$cache/build/expat" -G Ninja "-DCMAKE_INSTALL_PREFIX=$prefix" -DCMAKE_INSTALL_LIBDIR=lib -DCMAKE_BUILD_TYPE=Release -DEXPAT_SHARED_LIBS=OFF -DEXPAT_BUILD_TOOLS=OFF -DEXPAT_BUILD_EXAMPLES=OFF -DEXPAT_BUILD_TESTS=OFF -DEXPAT_BUILD_DOCS=OFF -DEXPAT_WITH_LIBBSD=OFF
    cmake --build "$cache/build/expat" --parallel "$jobs"
    cmake --install "$cache/build/expat"
    build_meson fontconfig -Dxml-backend=expat -Dfontations=disabled -Dnls=disabled -Ddoc=disabled -Dtests=disabled -Dtests-external-fonts=disabled -Dtools=disabled -Dcache-build=disabled
    ass_flags+=(-Dfontconfig=enabled -Ddirectwrite=disabled -Dcoretext=disabled)
    ;;
  windows-*) ass_flags+=(-Dfontconfig=disabled -Ddirectwrite=enabled -Dcoretext=disabled) ;;
  macos-*) ass_flags+=(-Dfontconfig=disabled -Ddirectwrite=disabled -Dcoretext=enabled) ;;
esac
build_meson libass "${ass_flags[@]}"

x264_source=("$cache"/source/x264/*)
(
  cd "${x264_source[0]}"
  ./configure "--prefix=$prefix" "${x264_flags[@]}" "--extra-cflags=$cflags" "--extra-ldflags=$ldflags"
  make -j"$jobs"
  make install
)

dav1d_source=("$cache"/source/dav1d/*)
if [[ -f "$cache/build/dav1d/meson-private/coredata.dat" ]]; then meson_flags+=(--wipe); fi
meson setup "$cache/build/dav1d" "${dav1d_source[0]}" "--prefix=$prefix" --libdir=lib "${meson_flags[@]}"
meson compile -C "$cache/build/dav1d" -j "$jobs"
meson install -C "$cache/build/dav1d"

ffmpeg_source=("$cache"/source/ffmpeg/*)
(
  cd "${ffmpeg_source[0]}"
  ./configure "--prefix=$prefix" "${ffmpeg_flags[@]}" "$tls_flag" "--extra-cflags=$cflags" "--extra-ldflags=$ldflags"
  make -j"$jobs"
  make install
)
cp "$prefix/bin/ffmpeg$suffix" "$prefix/bin/ffprobe$suffix" "$bundle/"
chmod +x "$bundle/ffmpeg$suffix" "$bundle/ffprobe$suffix"
{
  echo "Platform: $platform"
  "$compiler" --version
  meson --version
  "$bundle/ffmpeg$suffix" -version
  echo "x264 flags: ${x264_flags[*]}"
  echo "dav1d flags: ${meson_flags[*]}"
  echo "FFmpeg flags: ${ffmpeg_flags[*]} $tls_flag"
  echo "CFLAGS: $cflags"
  echo "LDFLAGS: $ldflags"
} > "$bundle/MEDIA-BUILD-INFO.txt"
