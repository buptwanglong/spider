import sys


def main():
    from spider.bin.cli import main as _main
    sys.exit(_main())


if __name__ == '__main__':  # pragma: no cover
    main()
