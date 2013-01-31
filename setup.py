#!/usr/bin/env python

from distutils.core import setup

setup(name='muto',
      version='0',
      description="",
      author='Joe Young',
      author_email='joe@jsyoung.us',
      url='http://www.jsyoung.us/code/',
      packages=['muto',
                'muto.storage',
                'muto.objects',
                'muto.accessories',
                'muto.accessories.decoders',
                'muto.accessories.decoders.profile',
                'muto.accessories.decoders.point',
                ],
     )

